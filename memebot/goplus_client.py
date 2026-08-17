"""GoPlus token security: batch fetch, three-state mapping, TTL cache."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx

from memebot.config import AppConfig
from memebot.store import Store

log = logging.getLogger("memebot.goplus")

BASE_URL = "https://api.gopluslabs.io"
TOKEN_PATH = "/api/v1/token"
ENDPOINTS = {
    "bsc": "/api/v1/token_security/56",
    "solana": "/api/v1/solana/token_security",
}

Status = Literal["pass", "reject", "transient"]
CreditFn = Callable[[], None]


@dataclass(frozen=True)
class SecurityVerdict:
    status: Status
    mintable: bool | None = None
    freezable: bool | None = None
    reason: str | None = None


@dataclass(frozen=True)
class GoPlusSettings:
    timeout_sec: float
    batch_size: int
    cache_ttl_min: float
    transient_ttl_sec: float
    max_tax_pct: float

    @classmethod
    def from_app(cls, cfg: AppConfig) -> GoPlusSettings:
        return cls(
            timeout_sec=float(cfg.get("security.timeout_sec")),
            batch_size=int(cfg.get("security.batch_size")),
            cache_ttl_min=float(cfg.get("security.cache_ttl_min")),
            transient_ttl_sec=float(cfg.get("security.transient_ttl_sec")),
            max_tax_pct=float(cfg.get("security.max_tax_pct")),
        )


def sign_access_token(app_key: str, ts: int, app_secret: str) -> str:
    return hashlib.sha1(f"{app_key}{ts}{app_secret}".encode()).hexdigest()


def authorization_header(token: str) -> str:
    """GoPlus access_token already includes the Bearer prefix."""
    if token.lower().startswith("bearer "):
        return token
    return f"Bearer {token}"


def _flag(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return str(value).strip() == "1"


def _authority_on(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, dict):
        if "status" not in value:
            return None
        return str(value["status"]).strip() == "1"
    return _flag(value)


def _tax(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tax_pct(value: Any) -> float | None:
    """GoPlus BSC tax is a ratio: 0.02 = 2%, 1 = 100%. Values > 1 are already percent."""
    n = _tax(value)
    if n is None:
        return None
    if n <= 1:
        return n * 100.0
    return n


def _optional_flag(value: Any) -> bool:
    if value is None or value == "":
        return False
    return _flag(value) is True


def _transfer_hooks(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _sol_fee_pct(item: dict[str, Any]) -> float | None:
    fee = item.get("transfer_fee")
    if not isinstance(fee, dict) or not fee:
        return None
    current = fee.get("current_fee_rate")
    if not isinstance(current, dict):
        return None
    rate = _tax(current.get("fee_rate"))
    if rate is None:
        return None
    return rate * 100.0


# #region agent log
def _dbg(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        import json as _json

        with open("/Users/yang/Documents/tgbot/.cursor/debug-1ea519.log", "a", encoding="utf-8") as _f:
            _f.write(
                _json.dumps(
                    {
                        "sessionId": "1ea519",
                        "hypothesisId": hypothesis_id,
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(time.time() * 1000),
                    },
                    default=str,
                )
                + "\n"
            )
    except Exception:
        pass


# #endregion


def _pick_item(result: dict[str, Any], network: str, address: str) -> dict[str, Any] | None:
    item = result.get(address)
    if isinstance(item, dict):
        return item
    if network == "bsc":
        lowered = address.lower()
        item = result.get(lowered)
        if isinstance(item, dict):
            return item
        for key, node in result.items():
            if isinstance(key, str) and key.lower() == lowered and isinstance(node, dict):
                return node
    return None


def map_solana(item: dict[str, Any], max_tax_pct: float) -> SecurityVerdict:
    closable = _authority_on(item.get("closable"))
    mutable = _authority_on(item.get("balance_mutable_authority"))
    default_state = item.get("default_account_state")
    non_transfer = item.get("non_transferable")
    if non_transfer is None or non_transfer == "":
        non_transfer = item.get("none_transferable")
    hooks = _transfer_hooks(item.get("transfer_hook"))
    fee_pct = _sol_fee_pct(item)
    if closable is None or mutable is None or default_state is None or default_state == "":
        verdict = SecurityVerdict("reject", reason="missing_field")
    elif closable:
        verdict = SecurityVerdict("reject", reason="closable")
    elif mutable:
        verdict = SecurityVerdict("reject", reason="balance_mutable")
    elif str(default_state).strip() == "2":
        verdict = SecurityVerdict("reject", reason="default_frozen")
    elif _optional_flag(non_transfer):
        verdict = SecurityVerdict("reject", reason="non_transferable")
    elif any(isinstance(h, dict) and h.get("address") for h in hooks):
        verdict = SecurityVerdict("reject", reason="transfer_hook")
    elif fee_pct is not None and fee_pct >= max_tax_pct:
        verdict = SecurityVerdict("reject", reason="transfer_fee")
    else:
        mintable = _authority_on(item.get("mintable"))
        freezable = _authority_on(item.get("freezable"))
        verdict = SecurityVerdict(
            "pass",
            mintable=bool(mintable),
            freezable=bool(freezable),
        )
    # #region agent log
    _dbg(
        "A",
        "goplus_client.py:map_solana",
        "solana hard fields",
        {
            "closable": closable,
            "balance_mutable": mutable,
            "default_account_state": default_state,
            "non_transferable": item.get("non_transferable"),
            "none_transferable": item.get("none_transferable"),
            "transfer_hook_n": len(hooks),
            "transfer_fee": item.get("transfer_fee"),
            "fee_pct": fee_pct,
            "max_tax_pct": max_tax_pct,
            "metadata_mutable": item.get("metadata_mutable"),
            "verdict": verdict.status,
            "reason": verdict.reason,
        },
    )
    # #endregion
    return verdict


def map_bsc(item: dict[str, Any], max_tax_pct: float) -> SecurityVerdict:
    honeypot = _flag(item.get("is_honeypot"))
    pausable = _flag(item.get("transfer_pausable"))
    mintable = _flag(item.get("is_mintable"))
    owner_chg = _flag(item.get("owner_change_balance"))
    buy_tax = _tax_pct(item.get("buy_tax"))
    sell_tax = _tax_pct(item.get("sell_tax"))
    if None in (honeypot, pausable, mintable, owner_chg, buy_tax, sell_tax):
        verdict = SecurityVerdict("reject", reason="missing_field")
    else:
        # GoPlus omits cannot_sell_all / cannot_buy when the contract has no such function.
        cannot_sell = item.get("cannot_sell_all")
        if cannot_sell is not None and cannot_sell != "":
            flag = _flag(cannot_sell)
            if flag is None:
                verdict = SecurityVerdict("reject", reason="missing_field")
            elif flag:
                verdict = SecurityVerdict("reject", reason="cannot_sell_all")
            else:
                verdict = None
        else:
            verdict = None
        if verdict is None and honeypot:
            verdict = SecurityVerdict("reject", reason="honeypot")
        elif verdict is None and _optional_flag(item.get("cannot_buy")):
            verdict = SecurityVerdict("reject", reason="cannot_buy")
        elif verdict is None and pausable:
            verdict = SecurityVerdict("reject", reason="transfer_pausable")
        elif verdict is None:
            assert buy_tax is not None and sell_tax is not None
            if buy_tax >= max_tax_pct or sell_tax >= max_tax_pct:
                verdict = SecurityVerdict("reject", reason="tax")
            elif mintable:
                verdict = SecurityVerdict("reject", reason="mintable")
            elif owner_chg:
                verdict = SecurityVerdict("reject", reason="owner_change_balance")
            elif _optional_flag(item.get("selfdestruct")):
                verdict = SecurityVerdict("reject", reason="selfdestruct")
            elif _optional_flag(item.get("personal_slippage_modifiable")):
                verdict = SecurityVerdict("reject", reason="personal_slippage")
            else:
                verdict = SecurityVerdict("pass")
    # #region agent log
    _dbg(
        "B",
        "goplus_client.py:map_bsc",
        "bsc hard fields",
        {
            "is_honeypot": item.get("is_honeypot"),
            "cannot_buy": item.get("cannot_buy"),
            "cannot_sell_all": item.get("cannot_sell_all"),
            "transfer_pausable": item.get("transfer_pausable"),
            "buy_tax": item.get("buy_tax"),
            "sell_tax": item.get("sell_tax"),
            "buy_tax_num": buy_tax,
            "sell_tax_num": sell_tax,
            "tax_unit": "pct",
            "max_tax_pct": max_tax_pct,
            "is_mintable": item.get("is_mintable"),
            "owner_change_balance": item.get("owner_change_balance"),
            "selfdestruct": item.get("selfdestruct"),
            "personal_slippage_modifiable": item.get("personal_slippage_modifiable"),
            "is_blacklisted": item.get("is_blacklisted"),
            "creator_percent": item.get("creator_percent"),
            "verdict": verdict.status,
            "reason": verdict.reason,
        },
    )
    # #endregion
    return verdict


def map_address(
    network: str,
    address: str,
    payload: dict[str, Any],
    max_tax_pct: float,
) -> SecurityVerdict:
    result = payload.get("result")
    if not isinstance(result, dict):
        return SecurityVerdict("reject", reason="not_in_result")
    item = _pick_item(result, network, address)
    if item is None:
        return SecurityVerdict("reject", reason="not_in_result")
    if network == "solana":
        return map_solana(item, max_tax_pct)
    return map_bsc(item, max_tax_pct)


def verdict_from_row(row: Any) -> SecurityVerdict:
    mintable = None if row["mintable"] is None else bool(row["mintable"])
    freezable = None if row["freezable"] is None else bool(row["freezable"])
    return SecurityVerdict(str(row["status"]), mintable=mintable, freezable=freezable)  # type: ignore[arg-type]


class GoPlusClient:
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        settings: GoPlusSettings,
        store: Store,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        record_credit: CreditFn | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self._app_key = app_key
        self._app_secret = app_secret
        self._record_credit = record_credit
        self._token: str | None = None
        self._token_deadline = 0.0
        self._token_lock = asyncio.Lock()
        timeout = httpx.Timeout(settings.timeout_sec)
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "memebot/1.0",
            },
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> GoPlusClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def cached(self, network: str, address: str) -> SecurityVerdict | None:
        row = self.store.get_security(network, address)
        if row is None:
            return None
        return verdict_from_row(row)

    def _remember(self, network: str, address: str, verdict: SecurityVerdict) -> None:
        now = datetime.now(UTC)
        if verdict.status == "transient":
            expires = now + timedelta(seconds=self.settings.transient_ttl_sec)
        else:
            expires = now + timedelta(minutes=self.settings.cache_ttl_min)
        self.store.put_security(
            network,
            address,
            verdict.status,
            expires.isoformat(),
            mintable=verdict.mintable,
            freezable=verdict.freezable,
        )

    async def _ensure_token(self) -> str | None:
        now = time.monotonic()
        if self._token is not None and now < self._token_deadline:
            return self._token
        async with self._token_lock:
            now = time.monotonic()
            if self._token is not None and now < self._token_deadline:
                return self._token
            ts = int(time.time())
            body = {
                "app_key": self._app_key,
                "time": ts,
                "sign": sign_access_token(self._app_key, ts, self._app_secret),
            }
            try:
                resp = await self._client.post(TOKEN_PATH, json=body)
            except httpx.RequestError as exc:
                log.warning("goplus token network error: %s", exc)
                self._token = None
                return None
            if resp.status_code >= 400:
                log.warning("goplus token HTTP %s", resp.status_code)
                self._token = None
                return None
            try:
                payload = resp.json()
            except ValueError:
                log.warning("goplus token invalid json")
                self._token = None
                return None
            result = payload.get("result") if isinstance(payload, dict) else None
            token = result.get("access_token") if isinstance(result, dict) else None
            expires = result.get("expires_in") if isinstance(result, dict) else None
            if (
                not isinstance(payload, dict)
                or payload.get("code") != 1
                or not isinstance(token, str)
                or not token
            ):
                code = payload.get("code") if isinstance(payload, dict) else None
                log.warning("goplus token unexpected payload code=%s", code)
                self._token = None
                return None
            ttl = float(expires) if isinstance(expires, (int, float)) and expires > 0 else 3600.0
            skew = min(60.0, max(1.0, ttl * 0.1))
            self._token = token
            self._token_deadline = time.monotonic() + max(1.0, ttl - skew)
            return self._token

    async def _request(self, network: str, addresses: Sequence[str]) -> dict[str, Any] | None:
        token = await self._ensure_token()
        if token is None:
            return None
        path = ENDPOINTS[network]
        if self._record_credit is not None:
            self._record_credit()
        headers = {"Authorization": authorization_header(token)}
        try:
            resp = await self._client.get(
                path,
                params={"contract_addresses": ",".join(addresses)},
                headers=headers,
            )
        except httpx.RequestError as exc:
            log.warning("goplus %s network error: %s", network, exc)
            return None
        if resp.status_code == 401:
            self._token = None
            self._token_deadline = 0.0
            token = await self._ensure_token()
            if token is None:
                return None
            try:
                resp = await self._client.get(
                    path,
                    params={"contract_addresses": ",".join(addresses)},
                    headers={"Authorization": authorization_header(token)},
                )
            except httpx.RequestError as exc:
                log.warning("goplus %s network error: %s", network, exc)
                return None
        if resp.status_code >= 400:
            log.warning("goplus %s HTTP %s", network, resp.status_code)
            return None
        try:
            payload = resp.json()
        except ValueError:
            log.warning("goplus %s invalid json", network)
            return None
        if not isinstance(payload, dict) or payload.get("code") != 1:
            code = payload.get("code") if isinstance(payload, dict) else None
            log.warning("goplus %s unexpected payload code=%s", network, code)
            return None
        return payload

    async def check_many(
        self, network: str, addresses: Sequence[str]
    ) -> dict[str, SecurityVerdict]:
        out: dict[str, SecurityVerdict] = {}
        need: list[str] = []
        for addr in addresses:
            hit = self.cached(network, addr)
            if hit is not None:
                out[addr] = hit
            else:
                need.append(addr)
        if not need:
            return out
        size = max(1, self.settings.batch_size)
        for i in range(0, len(need), size):
            batch = need[i : i + size]
            payload = await self._request(network, batch)
            if payload is None:
                for addr in batch:
                    verdict = SecurityVerdict("transient", reason="http")
                    self._remember(network, addr, verdict)
                    out[addr] = verdict
                continue
            for addr in batch:
                verdict = map_address(network, addr, payload, self.settings.max_tax_pct)
                self._remember(network, addr, verdict)
                out[addr] = verdict
                # #region agent log
                item = _pick_item(payload.get("result") or {}, network, addr)
                _dbg(
                    "D",
                    "goplus_client.py:check_many",
                    "mapped live address",
                    {
                        "network": network,
                        "addr_tail": addr[-8:] if isinstance(addr, str) and len(addr) >= 8 else addr,
                        "verdict": verdict.status,
                        "reason": verdict.reason,
                        "keys": sorted(item.keys()) if isinstance(item, dict) else [],
                    },
                )
                # #endregion
        return out
