"""GoPlus token security: batch fetch, three-state mapping, TTL cache."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Callable, Mapping, Sequence
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
    min_lp_locked_pct: float | None = None
    max_top_holder_pct: float | None = None
    max_top10_holder_pct: float | None = None
    max_creator_pct: float | None = None

    @classmethod
    def from_app(cls, cfg: AppConfig) -> GoPlusSettings:
        min_lp = cfg.get("security.min_lp_locked_pct")
        max_top = cfg.get("security.max_top_holder_pct")
        max_top10 = cfg.get("security.max_top10_holder_pct")
        max_creator = cfg.get("security.max_creator_pct")
        return cls(
            timeout_sec=float(cfg.get("security.timeout_sec")),
            batch_size=int(cfg.get("security.batch_size")),
            cache_ttl_min=float(cfg.get("security.cache_ttl_min")),
            transient_ttl_sec=float(cfg.get("security.transient_ttl_sec")),
            max_tax_pct=float(cfg.get("security.max_tax_pct")),
            min_lp_locked_pct=None if min_lp is None else float(min_lp),
            max_top_holder_pct=None if max_top is None else float(max_top),
            max_top10_holder_pct=None if max_top10 is None else float(max_top10),
            max_creator_pct=None if max_creator is None else float(max_creator),
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


_BURN_EVM = frozenset(
    {
        "0x000000000000000000000000000000000000dead",
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000001",
    }
)
_BURN_SOL = frozenset(
    {
        "1nc1nerator11111111111111111111111111111111",
    }
)
_SKIP_TAGS = (
    "pancake",
    "uniswap",
    "sushi",
    "null address",
    "raydium",
    "orca",
    "meteora",
    "pumpswap",
)


def _holder_addr(node: dict[str, Any]) -> str:
    for key in ("address", "account", "token_account"):
        value = node.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _addr_key(addr: str) -> str:
    if addr.startswith(("0x", "0X")):
        return addr.lower()
    return addr


def _creator_held_pct(item: dict[str, Any]) -> float | None:
    """BSC `creator_percent`; Solana matches `creators[].address` against `holders`."""
    direct = _frac_pct(item.get("creator_percent"))
    if direct is not None:
        return direct
    creators = _dict_nodes(item.get("creators"))
    if not creators:
        return None
    addrs = {_addr_key(addr) for node in creators if (addr := _holder_addr(node))}
    if not addrs:
        return None
    holders = _dict_nodes(item.get("holders"))
    if not holders:
        return None
    total = 0.0
    matched = False
    for node in holders:
        addr = _holder_addr(node)
        if not addr or _addr_key(addr) not in addrs:
            continue
        pct = _frac_pct(node.get("percent"))
        if pct is None:
            continue
        matched = True
        total += pct
    if not matched:
        return None
    return min(100.0, total)


def _is_burn_addr(addr: str) -> bool:
    if addr in _BURN_SOL:
        return True
    lowered = addr.lower()
    if lowered in _BURN_EVM:
        return True
    return lowered.startswith("0x") and len(lowered) == 42 and set(lowered[2:]) <= {"0"}


def _is_burn_tag(tag: Any) -> bool:
    return "burn" in str(tag or "").lower()


def _is_skip_tag(tag: Any) -> bool:
    text = str(tag or "").lower()
    return any(part in text for part in _SKIP_TAGS)


def _is_locked_flag(value: Any) -> bool:
    if value is True or value == 1:
        return True
    return str(value).strip() == "1"


def _frac_pct(value: Any) -> float | None:
    n = _tax(value)
    if n is None or n < 0:
        return None
    if n <= 1:
        return n * 100.0
    return n


def _dict_nodes(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [node for node in value if isinstance(node, dict)]


def _dex_burn_pct(item: dict[str, Any]) -> float | None:
    dex = item.get("dex")
    if not isinstance(dex, list):
        return None
    burns: list[float] = []
    for node in dex:
        if not isinstance(node, dict):
            continue
        burn = _tax(node.get("burn_percent"))
        if burn is None or burn < 0:
            continue
        burns.append(burn)
    if not burns:
        return None
    return max(burns)


def _dex_burn_for_pool(item: dict[str, Any], pool_address: str | None) -> float | None:
    """Burn % for the radar pool only. Never use a different pool's burn to pass."""
    dex = item.get("dex")
    if not isinstance(dex, list):
        return None
    nodes = [node for node in dex if isinstance(node, dict)]
    needle = (pool_address or "").strip()
    if needle:
        nodes = [node for node in nodes if str(node.get("id") or "").strip() == needle]
        if not nodes:
            return None
    burns: list[float] = []
    for node in nodes:
        burn = _tax(node.get("burn_percent"))
        if burn is None or burn < 0:
            continue
        burns.append(burn)
    if not burns:
        return None
    if needle:
        return burns[0]
    if len(burns) == 1:
        return burns[0]
    return None


def _lp_holders_locked_pct(item: dict[str, Any]) -> float | None:
    nodes = _dict_nodes(item.get("lp_holders"))
    if not nodes:
        return None
    locked = 0.0
    for node in nodes:
        pct = _frac_pct(node.get("percent"))
        if pct is None:
            continue
        addr = _holder_addr(node)
        if (
            _is_locked_flag(node.get("is_locked"))
            or _is_burn_addr(addr)
            or _is_burn_tag(node.get("tag"))
        ):
            locked += pct
    return min(100.0, locked)


def lp_locked_pct(
    item: dict[str, Any],
    *,
    prefer_dex_burn: bool = False,
    pool_address: str | None = None,
) -> float | None:
    if prefer_dex_burn:
        dex = _dex_burn_for_pool(item, pool_address)
        holders = _lp_holders_locked_pct(item)
        vals = [v for v in (dex, holders) if v is not None]
        if not vals:
            return None
        return max(vals)
    holders = _lp_holders_locked_pct(item)
    if holders is not None:
        return holders
    return _dex_burn_pct(item)


def _free_holder_pcts(item: dict[str, Any]) -> list[float] | None:
    holders = _dict_nodes(item.get("holders"))
    if not holders:
        return None
    lp_addrs = {
        _addr_key(_holder_addr(node))
        for node in _dict_nodes(item.get("lp_holders"))
        if _holder_addr(node)
    }
    pcts: list[float] = []
    for node in holders:
        addr = _holder_addr(node)
        if not addr:
            continue
        key = _addr_key(addr)
        if (
            _is_burn_addr(addr)
            or _is_locked_flag(node.get("is_locked"))
            or _is_skip_tag(node.get("tag"))
            or _is_burn_tag(node.get("tag"))
            or key in lp_addrs
        ):
            continue
        pct = _frac_pct(node.get("percent"))
        if pct is None:
            continue
        pcts.append(pct)
    return pcts


def top_free_holder_pct(item: dict[str, Any]) -> float | None:
    pcts = _free_holder_pcts(item)
    if pcts is None:
        return None
    return max(pcts) if pcts else 0.0


def top10_free_holder_pct(item: dict[str, Any]) -> float | None:
    pcts = _free_holder_pcts(item)
    if pcts is None:
        return None
    return min(100.0, sum(sorted(pcts, reverse=True)[:10]))


def _owner_still_active(item: dict[str, Any]) -> bool:
    if "owner_address" not in item:
        return True
    addr = item.get("owner_address")
    if not isinstance(addr, str) or not addr.strip():
        return False
    return not _is_burn_addr(addr.strip())


def _distribution_verdict(
    item: dict[str, Any],
    *,
    required: bool,
    min_lp_locked_pct: float | None,
    max_top_holder_pct: float | None,
    max_top10_holder_pct: float | None,
    max_creator_pct: float | None,
    prefer_dex_burn: bool = False,
    pool_address: str | None = None,
) -> SecurityVerdict | None:
    if (
        min_lp_locked_pct is None
        and max_top_holder_pct is None
        and max_top10_holder_pct is None
        and max_creator_pct is None
    ):
        return None
    locked = lp_locked_pct(item, prefer_dex_burn=prefer_dex_burn, pool_address=pool_address)
    top = top_free_holder_pct(item)
    top10 = top10_free_holder_pct(item)
    reason: str | None = None
    if min_lp_locked_pct is not None:
        if locked is None:
            if required:
                reason = "missing_field"
        elif locked < min_lp_locked_pct:
            reason = "lp_unlocked"
    if reason is None and max_top_holder_pct is not None:
        if top is None:
            if required:
                reason = "missing_field"
        elif top > max_top_holder_pct:
            reason = "holder_concentration"
    if reason is None and max_top10_holder_pct is not None:
        if top10 is None:
            if required:
                reason = "missing_field"
        elif top10 > max_top10_holder_pct:
            reason = "top10_concentration"
    if reason is None and max_creator_pct is not None:
        creator = _creator_held_pct(item)
        if creator is not None and creator > max_creator_pct:
            reason = "creator_concentration"
    if reason is None:
        return None
    return SecurityVerdict("reject", reason=reason)


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


def solana_authority_open(
    mintable: bool | None,
    freezable: bool | None,
    network: str,
) -> bool:
    """True when Solana mint/freeze is still on or unknown (cannot grade strong)."""
    if network != "solana":
        return False
    return mintable is not False or freezable is not False


def goplus_ready(network: str, verdict: SecurityVerdict) -> bool:
    """True only for a complete GoPlus pass. Reject, transient, and unknown fields cannot push."""
    if verdict.status != "pass":
        return False
    if network == "solana":
        return verdict.mintable is False and verdict.freezable is False
    return True


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


def map_solana(
    item: dict[str, Any],
    max_tax_pct: float,
    *,
    min_lp_locked_pct: float | None = None,
    max_top_holder_pct: float | None = None,
    max_top10_holder_pct: float | None = None,
    max_creator_pct: float | None = None,
    pool_address: str | None = None,
) -> SecurityVerdict:
    closable = _authority_on(item.get("closable"))
    mutable = _authority_on(item.get("balance_mutable_authority"))
    default_state = item.get("default_account_state")
    non_transfer = item.get("non_transferable")
    if non_transfer is None or non_transfer == "":
        non_transfer = item.get("none_transferable")
    hooks = _transfer_hooks(item.get("transfer_hook"))
    fee_pct = _sol_fee_pct(item)
    if closable is None or mutable is None or default_state is None or default_state == "":
        return SecurityVerdict("reject", reason="missing_field")
    if closable:
        return SecurityVerdict("reject", reason="closable")
    if mutable:
        return SecurityVerdict("reject", reason="balance_mutable")
    if str(default_state).strip() == "2":
        return SecurityVerdict("reject", reason="default_frozen")
    if _optional_flag(non_transfer):
        return SecurityVerdict("reject", reason="non_transferable")
    if any(isinstance(h, dict) and h.get("address") for h in hooks):
        return SecurityVerdict("reject", reason="transfer_hook")
    if fee_pct is not None and fee_pct >= max_tax_pct:
        return SecurityVerdict("reject", reason="transfer_fee")
    mintable = _authority_on(item.get("mintable"))
    freezable = _authority_on(item.get("freezable"))
    metadata = _authority_on(item.get("metadata_mutable"))
    if mintable is None or freezable is None or metadata is None:
        return SecurityVerdict("reject", reason="missing_field")
    if metadata:
        return SecurityVerdict("reject", reason="metadata_mutable")
    if mintable:
        return SecurityVerdict("reject", reason="mintable")
    if freezable:
        return SecurityVerdict("reject", reason="freezable")
    dist = _distribution_verdict(
        item,
        required=False,
        min_lp_locked_pct=min_lp_locked_pct,
        max_top_holder_pct=max_top_holder_pct,
        max_top10_holder_pct=max_top10_holder_pct,
        max_creator_pct=max_creator_pct,
        prefer_dex_burn=True,
        pool_address=pool_address,
    )
    if dist is not None:
        return dist
    return SecurityVerdict("pass", mintable=mintable, freezable=freezable)


def map_bsc(
    item: dict[str, Any],
    max_tax_pct: float,
    *,
    min_lp_locked_pct: float | None = None,
    max_top_holder_pct: float | None = None,
    max_top10_holder_pct: float | None = None,
    max_creator_pct: float | None = None,
) -> SecurityVerdict:
    open_src = _flag(item.get("is_open_source"))
    if open_src is None:
        return SecurityVerdict("reject", reason="missing_field")
    if not open_src:
        return SecurityVerdict("reject", reason="not_open_source")
    honeypot = _flag(item.get("is_honeypot"))
    pausable = _flag(item.get("transfer_pausable"))
    mintable = _flag(item.get("is_mintable"))
    owner_chg = _flag(item.get("owner_change_balance"))
    buy_tax = _tax_pct(item.get("buy_tax"))
    sell_tax = _tax_pct(item.get("sell_tax"))
    if (
        honeypot is None
        or pausable is None
        or mintable is None
        or owner_chg is None
        or buy_tax is None
        or sell_tax is None
    ):
        return SecurityVerdict("reject", reason="missing_field")
    # GoPlus omits cannot_sell_all / cannot_buy when the contract has no such function.
    cannot_sell = item.get("cannot_sell_all")
    if cannot_sell is not None and cannot_sell != "":
        flag = _flag(cannot_sell)
        if flag is None:
            return SecurityVerdict("reject", reason="missing_field")
        if flag:
            return SecurityVerdict("reject", reason="cannot_sell_all")
    if honeypot:
        return SecurityVerdict("reject", reason="honeypot")
    if _optional_flag(item.get("cannot_buy")):
        return SecurityVerdict("reject", reason="cannot_buy")
    if pausable:
        return SecurityVerdict("reject", reason="transfer_pausable")
    if buy_tax >= max_tax_pct or sell_tax >= max_tax_pct:
        return SecurityVerdict("reject", reason="tax")
    if mintable:
        return SecurityVerdict("reject", reason="mintable")
    if owner_chg:
        return SecurityVerdict("reject", reason="owner_change_balance")
    if _optional_flag(item.get("selfdestruct")):
        return SecurityVerdict("reject", reason="selfdestruct")
    if _optional_flag(item.get("personal_slippage_modifiable")):
        return SecurityVerdict("reject", reason="personal_slippage")
    hidden = _flag(item.get("hidden_owner"))
    if hidden is None:
        return SecurityVerdict("reject", reason="missing_field")
    if hidden:
        return SecurityVerdict("reject", reason="hidden_owner")
    take_back = _flag(item.get("can_take_back_ownership"))
    if take_back is None:
        return SecurityVerdict("reject", reason="missing_field")
    if take_back:
        return SecurityVerdict("reject", reason="take_back_ownership")
    blacklisted = _flag(item.get("is_blacklisted"))
    if blacklisted is None:
        return SecurityVerdict("reject", reason="missing_field")
    if blacklisted and _owner_still_active(item):
        return SecurityVerdict("reject", reason="blacklist")
    launch = item.get("launchpad_token")
    if isinstance(launch, dict) and str(launch.get("is_launchpad_token")).strip() == "1":
        name = str(launch.get("launchpad_name") or "").lower().replace("_", "-")
        if name in {"four.meme", "fourmeme", "four-meme"}:
            return SecurityVerdict("reject", reason="launchpad")
    dist = _distribution_verdict(
        item,
        required=True,
        min_lp_locked_pct=min_lp_locked_pct,
        max_top_holder_pct=max_top_holder_pct,
        max_top10_holder_pct=max_top10_holder_pct,
        max_creator_pct=max_creator_pct,
    )
    if dist is not None:
        return dist
    return SecurityVerdict("pass")


def map_address(
    network: str,
    address: str,
    payload: dict[str, Any],
    max_tax_pct: float,
    *,
    min_lp_locked_pct: float | None = None,
    max_top_holder_pct: float | None = None,
    max_top10_holder_pct: float | None = None,
    max_creator_pct: float | None = None,
    pool_address: str | None = None,
) -> SecurityVerdict:
    if network not in ENDPOINTS:
        return SecurityVerdict("reject", reason="unknown_network")
    result = payload.get("result")
    if not isinstance(result, dict):
        return SecurityVerdict("reject", reason="not_in_result")
    item = _pick_item(result, network, address)
    if item is None:
        return SecurityVerdict("reject", reason="not_in_result")
    if network == "solana":
        return map_solana(
            item,
            max_tax_pct,
            min_lp_locked_pct=min_lp_locked_pct,
            max_top_holder_pct=max_top_holder_pct,
            max_top10_holder_pct=max_top10_holder_pct,
            max_creator_pct=max_creator_pct,
            pool_address=pool_address,
        )
    return map_bsc(
        item,
        max_tax_pct,
        min_lp_locked_pct=min_lp_locked_pct,
        max_top_holder_pct=max_top_holder_pct,
        max_top10_holder_pct=max_top10_holder_pct,
        max_creator_pct=max_creator_pct,
    )


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
        path = ENDPOINTS.get(network)
        if path is None:
            log.warning("goplus unknown network %s", network)
            return None
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
        self,
        network: str,
        addresses: Sequence[str],
        *,
        pool_by_token: Mapping[str, str] | None = None,
    ) -> dict[str, SecurityVerdict]:
        out: dict[str, SecurityVerdict] = {}
        need: list[str] = []
        pools = pool_by_token or {}
        for addr in addresses:
            hit = self.cached(network, addr)
            if hit is not None and (hit.status != "pass" or goplus_ready(network, hit)):
                out[addr] = hit
            else:
                need.append(addr)
        if not need:
            return out
        if network not in ENDPOINTS:
            for addr in need:
                verdict = SecurityVerdict("reject", reason="unknown_network")
                self._remember(network, addr, verdict)
                out[addr] = verdict
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
                verdict = map_address(
                    network,
                    addr,
                    payload,
                    self.settings.max_tax_pct,
                    min_lp_locked_pct=self.settings.min_lp_locked_pct,
                    max_top_holder_pct=self.settings.max_top_holder_pct,
                    max_top10_holder_pct=self.settings.max_top10_holder_pct,
                    max_creator_pct=self.settings.max_creator_pct,
                    pool_address=pools.get(addr),
                )
                self._remember(network, addr, verdict)
                out[addr] = verdict
        return out
