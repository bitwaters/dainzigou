"""Telegram send path: template, state machine, retries, alert silence."""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx

from memebot.store import Store

log = logging.getLogger("memebot.notify")
_ZW = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f]")
_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def payload_from_signal_row(row: Any) -> dict[str, Any]:
    feats: dict[str, Any] = {}
    raw = row["features_json"] if "features_json" in row.keys() else None
    if raw:
        try:
            loaded = json.loads(str(raw))
            if isinstance(loaded, dict):
                feats = loaded
        except json.JSONDecodeError:
            feats = {}
    return {
        "symbol": str(feats.get("symbol") or "RESEND"),
        "network": row["network"],
        "token_address": row["token_address"],
        "pool_address": row["pool_id"],
        "created_at": str(row["created_at"]),
        "fdv_usd": (
            row["fdv_at_signal"] if row["fdv_at_signal"] is not None else feats.get("fdv_usd")
        ),
        "market_cap_usd": feats.get("market_cap_usd"),
        "reserve_usd": feats.get("reserve_usd"),
        "age_min": feats.get("age_min"),
        "holders": feats.get("holders"),
        "buyers": feats.get("buyers"),
        "sellers": feats.get("sellers"),
        "pool_buyers_m15": feats.get("pool_buyers_m15"),
        "buy_sell_ratio": feats.get("buy_sell_ratio"),
        "price_change_pct": feats.get("price_change_pct"),
        "price_change_usd": feats.get("price_change_usd"),
        "price_change_native": feats.get("price_change_native"),
        "dwell_sec": feats.get("dwell_sec"),
        "gt_score": feats.get("gt_score"),
        "mint_authority": feats.get("mint_authority"),
        "freeze_authority": feats.get("freeze_authority"),
        "honeypot": feats.get("honeypot"),
    }


def sanitize_chain_text(value: str, max_len: int) -> str:
    text = html.escape(value, quote=True)
    text = _ZW.sub("", text)
    text = _CTRL.sub("", text)
    return text[:max_len]


_CHAIN_LABEL = {"solana": "SOL", "bsc": "BSC"}
_GMGN_CHAIN = {"solana": "sol", "bsc": "bsc"}
_DBOT_CHAIN = {"solana": "solana", "bsc": "bsc"}
_PCT_ABS_MAX = 9999.0


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact(n: float, suffix: str) -> str:
    text = f"{n:.1f}{suffix}"
    return text.replace(f".0{suffix}", suffix)


def fmt_usd(value: Any) -> str | None:
    n = _as_float(value)
    if n is None:
        return None
    abs_n = abs(n)
    if abs_n < 1000:
        return f"${n:.0f}" if abs_n >= 100 else f"${n:.2f}"
    if abs_n < 1_000_000:
        return f"${_compact(n / 1000, 'K')}"
    if abs_n < 1_000_000_000:
        return f"${_compact(n / 1_000_000, 'M')}"
    return f"${_compact(n / 1_000_000_000, 'B')}"


def fmt_count(value: Any) -> str | None:
    n = _as_float(value)
    if n is None:
        return None
    if abs(n) < 1000:
        return f"{n:.0f}"
    if abs(n) < 1_000_000:
        return _compact(n / 1000, "K")
    return _compact(n / 1_000_000, "M")


def fmt_age(value: Any) -> str | None:
    n = _as_float(value)
    if n is None:
        return None
    if n < 60:
        return f"{n:.0f}min" if n < 10 else f"{n:.1f}min"
    if n < 60 * 48:
        return f"{n / 60:.1f}h"
    return f"{n / 60 / 24:.1f}d"


def fmt_pct(value: Any) -> str | None:
    n = _as_float(value)
    if n is None or abs(n) > _PCT_ABS_MAX:
        return None
    sign = "+" if n > 0 else ""
    return f"{sign}{n:.1f}%"


def fmt_dwell(value: Any) -> str | None:
    n = _as_float(value)
    if n is None:
        return None
    if n < 60:
        return f"{n:.0f}s"
    return f"{n / 60:.0f}min"


def fmt_ratio(value: Any) -> str | None:
    n = _as_float(value)
    if n is None:
        return None
    return f"{n:.1f}"


def fmt_clock(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return ts.astimezone(UTC).strftime("%H:%M:%S UTC")
    except ValueError:
        return raw


def _windows_line(chg: Any) -> str | None:
    if not isinstance(chg, dict):
        return None
    parts: list[str] = []
    any_num = False
    for key, label in (("m5", "5m"), ("m15", "15m"), ("h1", "1h")):
        shown = fmt_pct(chg.get(key))
        if shown:
            any_num = True
            parts.append(f"{label} {shown}")
        else:
            parts.append(f"{label} —")
    return " · ".join(parts) if any_num else None


def _security_line(payload: dict[str, Any], network: str) -> str:
    parts: list[str] = []
    if network != "solana":
        hp = payload.get("honeypot")
        if hp is True:
            parts.append("蜜罐 是")
        elif hp is False or hp in {"no", "false"}:
            parts.append("蜜罐 否")
        elif hp is not None:
            parts.append("蜜罐 未知")
    mint = payload.get("mint_authority")
    freeze = payload.get("freeze_authority")
    if mint == "no" and freeze == "no":
        parts.append("权限已弃权")
    gt = fmt_count(payload.get("gt_score"))
    if gt is not None:
        parts.append(f"GT {gt}")
    return " · ".join(parts)


def render_signal(payload: dict[str, Any], raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    max_len = int(raw["telegram"]["symbol_max_len"])
    symbol = sanitize_chain_text(str(payload.get("symbol") or "?"), max_len)
    network = str(payload.get("network") or "")
    chain = _CHAIN_LABEL.get(network, network.upper() or "?")
    ca = html.escape(str(payload.get("token_address") or ""), quote=True)
    age = fmt_age(payload.get("age_min")) or "—"
    lines = [f"🚀 确认  ${symbol}", f"{html.escape(chain)} · 池龄 {age}", ""]
    lines.append(f"<code>{ca}</code>")
    money: list[str] = []
    mc = fmt_usd(payload.get("market_cap_usd"))
    fdv = fmt_usd(payload.get("fdv_usd"))
    liq = fmt_usd(payload.get("reserve_usd"))
    if mc:
        money.append(f"市值 {mc}")
    if fdv:
        money.append(f"FDV {fdv}")
    if liq:
        money.append(f"流动性 {liq}")
    if money:
        lines.append("")
        lines.append("💰 " + " · ".join(money))
    holders = fmt_count(payload.get("holders"))
    if holders:
        lines.append(f"👥 持有人 {holders}")
    windows = _windows_line(payload.get("price_change_usd"))
    if windows:
        lines.append(f"📈 进盯时 {windows}")
    confirm: list[str] = []
    dwell = fmt_dwell(payload.get("dwell_sec"))
    if dwell:
        confirm.append(f"盯盘 {dwell}")
    buyers = payload.get("buyers")
    sellers = payload.get("sellers")
    if buyers is not None or sellers is not None:
        buy_s = buyers if buyers is not None else "—"
        sell_s = sellers if sellers is not None else "—"
        confirm.append(f"窗口买家 {buy_s} / 卖家 {sell_s}")
    pool_buyers = fmt_count(payload.get("pool_buyers_m15"))
    if pool_buyers:
        confirm.append(f"池15m买家 {pool_buyers}")
    ratio = fmt_ratio(payload.get("buy_sell_ratio"))
    if ratio:
        confirm.append(f"买卖比 {ratio}")
    chg = fmt_pct(payload.get("price_change_pct"))
    if chg:
        confirm.append(f"相对进场 {chg}")
    if confirm:
        lines.append("✅ " + " · ".join(confirm))
    security = _security_line(payload, network)
    if security:
        lines.append(f"🛡 {security}")
    clock = fmt_clock(payload.get("created_at"))
    if clock:
        lines.append("")
        lines.append(html.escape(clock))
    token_q = quote(str(payload.get("token_address") or ""), safe="")
    gmgn = _GMGN_CHAIN.get(network, network)
    dbot = _DBOT_CHAIN.get(network, network)
    markup = {
        "inline_keyboard": [
            [
                {"text": "GMGN", "url": f"https://gmgn.ai/{gmgn}/token/{token_q}"},
                {"text": "Debot", "url": f"https://dbotx.com/{dbot}/{token_q}"},
            ]
        ]
    }
    return "\n".join(lines), markup


class Notifier:
    def __init__(
        self,
        store: Store,
        raw: dict[str, Any],
        token: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Any = None,
    ) -> None:
        self.store = store
        self.raw = raw
        self.token = token
        self._silence: dict[str, datetime] = {}
        timeout = float(raw["telegram"]["send_timeout_sec"])
        self._client = httpx.AsyncClient(
            base_url="https://api.telegram.org",
            timeout=timeout,
            transport=transport,
        )
        self._sleep = sleep

    async def aclose(self) -> None:
        await self._client.aclose()

    def silenced(self, key: str, now: datetime) -> bool:
        window = float(self.raw["telegram"]["alert_silence_sec"])
        last = self._silence.get(key)
        if last and now - last < timedelta(seconds=window):
            return True
        self._silence[key] = now
        return False

    async def alert(self, text: str, key: str, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        if self.silenced(key, now):
            return
        await self._send(str(self.raw["telegram"]["admin_id"]), text, None)

    async def send_signal(self, signal_id: int, payload: dict[str, Any]) -> None:
        text, markup = render_signal(payload, self.raw)
        await self._deliver(signal_id, str(self.raw["telegram"]["channel_id"]), text, markup)

    async def _deliver(
        self,
        signal_id: int,
        chat_id: str,
        text: str,
        markup: dict[str, Any] | None,
    ) -> None:
        retry = self.raw["runtime"]["retry"]
        attempts = int(retry["max_attempts"])
        last_status = 0
        for i in range(attempts):
            try:
                resp = await self._client.post(
                    f"/bot{self.token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "reply_markup": markup,
                    },
                )
            except httpx.RequestError:
                last_status = 0
                if self._sleep:
                    await self._sleep(float(retry["backoff_base_sec"]))
                continue
            last_status = resp.status_code
            if resp.status_code == 429:
                wait = float(resp.headers.get("retry-after") or retry["backoff_base_sec"])
                if self._sleep:
                    await self._sleep(wait)
                continue
            if 500 <= resp.status_code <= 599:
                if self._sleep:
                    await self._sleep(float(retry["backoff_base_sec"]))
                continue
            if 400 <= resp.status_code <= 499:
                self.store.update_signal_status(signal_id, "failed_perm", attempts=i + 1)
                await self.alert(
                    f"信号 {signal_id} 发送 4xx={resp.status_code}", f"tg.4xx.{signal_id}"
                )
                return
            body = resp.json()
            mid = None
            if isinstance(body, dict):
                mid = str((body.get("result") or {}).get("message_id") or "")
            self.store.update_signal_status(
                signal_id, "sent", telegram_message_id=mid, attempts=i + 1
            )
            return
        status = "failed_retry" if last_status == 0 or last_status >= 500 else "failed_perm"
        self.store.update_signal_status(signal_id, status, attempts=attempts)

    async def send_admin(self, text: str) -> None:
        await self._send(str(self.raw["telegram"]["admin_id"]), text, None)

    async def get_updates(self, offset: int | None = None, timeout: int = 0) -> list[Any]:
        params: dict[str, Any] = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        try:
            resp = await self._client.get(f"/bot{self.token}/getUpdates", params=params)
        except httpx.HTTPError as exc:
            log.warning("getUpdates failed: %s", exc)
            return []
        if resp.status_code != 200:
            log.warning("getUpdates HTTP %s", resp.status_code)
            return []
        body = resp.json()
        if not isinstance(body, dict):
            return []
        result = body.get("result")
        return result if isinstance(result, list) else []

    async def _send(self, chat_id: str, text: str, markup: dict[str, Any] | None) -> None:
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if markup is not None:
            body["reply_markup"] = markup
        try:
            await self._client.post(f"/bot{self.token}/sendMessage", json=body)
        except httpx.HTTPError as exc:
            log.warning("telegram alert failed: %s", exc)

    def try_insert_pending(
        self,
        *,
        network: str,
        token_address: str,
        pool_id: str,
        created_at: str,
        config_hash: str,
        payload: dict[str, Any],
        score: float | None,
        features: dict[str, Any],
        price: float | None,
        fdv: float | None,
    ) -> int | None:
        kind = "confirmed"
        if self.store.has_live_signal(network, token_address, kind):
            return None
        max_fail = int(self.raw["telegram"]["max_send_failures"])
        if self.store.count_send_failures(network, token_address, kind) >= max_fail:
            return None
        return self.store.insert_signal_with_event(
            network=network,
            token_address=token_address,
            pool_id=pool_id,
            kind=kind,
            status="pending",
            created_at=created_at,
            config_hash=config_hash,
            event_type="signal.confirmed",
            payload=json.dumps(payload, ensure_ascii=True),
            score=score,
            features_json=json.dumps(features, ensure_ascii=True),
            price_at_signal=price,
            fdv_at_signal=fdv,
        )

    def reconcile_startup(self, now: datetime, config_hash: str) -> tuple[list[int], bool]:
        ttl = float(self.raw["telegram"]["pending_ttl_sec"])
        cutoff = (now.astimezone(UTC) - timedelta(seconds=ttl)).isoformat()
        abandoned = self.store.abandon_expired_pending(cutoff)
        prev = self.store.kv_get("last_config_hash")
        first_start = prev is None
        changed = prev is not None and prev != config_hash
        if first_start or changed:
            self.store.kv_set("last_config_hash", config_hash)
        return abandoned, changed
