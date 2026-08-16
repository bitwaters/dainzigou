"""Telegram cards, dual-channel routing, send state machine, startup reconcile."""

from __future__ import annotations

import html
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
_CHAIN_LABEL = {"solana": "SOL", "bsc": "BSC"}
_GMGN_CHAIN = {"solana": "sol", "bsc": "bsc"}
_DBOT_CHAIN = {"solana": "solana", "bsc": "bsc"}
_PCT_ABS_MAX = 9999.0


def sanitize_chain_text(value: str, max_len: int) -> str:
    text = html.escape(value, quote=True)
    text = _ZW.sub("", text)
    text = _CTRL.sub("", text)
    return text[:max_len]


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


def fmt_pct(value: Any) -> str | None:
    n = _as_float(value)
    if n is None or abs(n) > _PCT_ABS_MAX:
        return None
    sign = "+" if n > 0 else ""
    return f"{sign}{n:.1f}%"


def fmt_clock(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return raw


def channel_id(raw: dict[str, Any], grade: str) -> str:
    if grade == "strong":
        return str(raw["telegram"]["strong_channel_id"])
    return str(raw["telegram"]["weak_channel_id"])


def card_from_job(
    *,
    grade: str,
    pool: Any,
    metrics: Any,
    job: Any,
    now: datetime,
    require_net_buy: bool,
) -> dict[str, Any]:
    changes = pool.price_change_usd or {}
    mintable = bool(getattr(job.verdict, "mintable", False))
    freezable = bool(getattr(job.verdict, "freezable", False))
    authority = mintable or freezable
    return {
        "grade": grade,
        "network": pool.network,
        "symbol": pool.symbol,
        "name": pool.name,
        "token_address": pool.token_address,
        "chg_1m": metrics.chg_1m,
        "m5": changes.get("m5"),
        "h1": changes.get("h1"),
        "dist": metrics.dist,
        "buy_usd": job.buy_usd,
        "sell_usd": job.sell_usd,
        "hide_net_buy": not require_net_buy,
        "reserve_usd": pool.reserve_usd,
        "fdv_usd": pool.fdv_usd,
        "market_cap_usd": pool.market_cap_usd,
        "created_at": now.isoformat(),
        "authority_open": authority and pool.network == "solana",
    }


def render_card(payload: dict[str, Any], raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    max_len = int(raw["telegram"]["symbol_max_len"])
    symbol = sanitize_chain_text(str(payload.get("symbol") or "?"), max_len)
    name = sanitize_chain_text(str(payload.get("name") or ""), max_len)
    grade = str(payload.get("grade") or "")
    label = "强" if grade == "strong" else "弱"
    network = str(payload.get("network") or "")
    chain = _CHAIN_LABEL.get(network, network.upper() or "?")
    ca = html.escape(str(payload.get("token_address") or ""), quote=True)
    title = f"{label} ${symbol}"
    if name and name != symbol:
        title += f" · {name}"
    lines = [
        title,
        html.escape(chain),
        f"<code>{ca}</code>",
        "",
        (
            f"1m {fmt_pct(payload.get('chg_1m')) or '—'} · "
            f"m5 {fmt_pct(payload.get('m5')) or '—'} · "
            f"h1 {fmt_pct(payload.get('h1')) or '—'} · "
            f"dist {fmt_pct(payload.get('dist')) or '—'}"
        ),
    ]
    if payload.get("hide_net_buy"):
        lines.append("近窗买 — · 卖 —")
    else:
        buy = fmt_usd(payload.get("buy_usd")) or "$0"
        sell = fmt_usd(payload.get("sell_usd")) or "$0"
        lines.append(f"近窗买 {buy} · 卖 {sell}")
    liq = fmt_usd(payload.get("reserve_usd")) or "—"
    fdv = fmt_usd(payload.get("fdv_usd")) or "—"
    mc = fmt_usd(payload.get("market_cap_usd"))
    lines.append(f"流动性 {liq} · FDV {fdv} · 市值 {mc if mc else '—'}")
    clock = fmt_clock(payload.get("created_at"))
    if clock:
        lines.append(clock)
    security = "安全通过"
    if network == "solana":
        security += " · 权限未弃权" if payload.get("authority_open") else " · 权限已弃权"
    lines.append(security)
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
        admin_id: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Any = None,
    ) -> None:
        self.store = store
        self.raw = raw
        self.token = token
        self.admin_id = admin_id
        self._silence: dict[str, datetime] = {}
        timeout = float(raw["telegram"]["send_timeout_sec"])
        self._client = httpx.AsyncClient(
            base_url="https://api.telegram.org",
            timeout=timeout,
            transport=transport,
        )
        self._sleep = sleep
        self.last_chat_id: str | None = None

    async def aclose(self) -> None:
        await self._client.aclose()

    def silenced(self, key: str, now: datetime) -> bool:
        window = float(self.raw["telegram"]["alert_silence_sec"])
        last = self._silence.get(key)
        if last and now - last < timedelta(seconds=window):
            return True
        self._silence[key] = now
        return False

    async def alert(self, message: str, key: str, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        if self.silenced(key, now):
            return
        await self._send(self.admin_id, message, None)

    async def send_grade(self, **payload: Any) -> bool:
        signal_id = int(payload["signal_id"])
        if "card" in payload:
            card = dict(payload["card"])
        else:
            card = card_from_job(
                grade=str(payload["grade"]),
                pool=payload["pool"],
                metrics=payload["metrics"],
                job=payload["job"],
                now=payload["now"],
                require_net_buy=bool(self.raw["grade"]["require_net_buy"]),
            )
        self.store.put_signal_card(signal_id, card)
        text, markup = render_card(card, self.raw)
        chat = channel_id(self.raw, str(card.get("grade") or payload.get("grade") or ""))
        return await self._deliver(signal_id, chat, text, markup)

    async def resume_pending(self) -> None:
        for row in self.store.list_pending():
            signal_id = int(row["id"])
            card = self.store.get_signal_card(signal_id) or {
                "grade": row["grade"],
                "network": row["network"],
                "token_address": row["token_address"],
                "symbol": "?",
                "created_at": row["created_at"],
                "fdv_usd": row["fdv_usd"],
                "hide_net_buy": not bool(self.raw["grade"]["require_net_buy"]),
            }
            text, markup = render_card(card, self.raw)
            await self._deliver(signal_id, channel_id(self.raw, str(row["grade"])), text, markup)

    async def _deliver(
        self,
        signal_id: int,
        chat_id: str,
        text: str,
        markup: dict[str, Any] | None,
    ) -> bool:
        self.last_chat_id = chat_id
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
                self.store.update_signal_status(signal_id, "failed_perm", add_fails=i + 1)
                await self.alert(
                    f"信号 {signal_id} 发送 4xx={resp.status_code}",
                    f"tg.4xx.{signal_id}",
                )
                return False
            body = resp.json()
            tg_ok = body.get("ok") if isinstance(body, dict) else None
            mid = ""
            if isinstance(body, dict):
                mid = str((body.get("result") or {}).get("message_id") or "")
            will_mark_sent = tg_ok is True
            if not will_mark_sent:
                err = 0
                if isinstance(body, dict):
                    raw_err = body.get("error_code")
                    if isinstance(raw_err, int):
                        err = raw_err
                if 400 <= err <= 499 or err == 0:
                    self.store.update_signal_status(signal_id, "failed_perm", add_fails=i + 1)
                    await self.alert(
                        f"信号 {signal_id} 发送 ok=false code={err}",
                        f"tg.okfalse.{signal_id}",
                    )
                    return False
                last_status = err or last_status
                continue
            row = self.store.get_signal(signal_id)
            if row is None:
                raise RuntimeError(f"signal {signal_id} missing after send")
            self.store.mark_signal_sent(
                signal_id,
                f"signal.{row['grade']}",
                {"id": signal_id, "message_id": mid, "chat_id": chat_id},
            )
            return True
        status = "failed_retry" if last_status == 0 or last_status >= 500 else "failed_perm"
        self.store.update_signal_status(signal_id, status, add_fails=attempts)
        return False

    async def send_admin(self, text: str) -> None:
        await self._send(self.admin_id, text, None)

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
        body: dict[str, Any] = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if markup is not None:
            body["reply_markup"] = markup
        try:
            await self._client.post(f"/bot{self.token}/sendMessage", json=body)
        except httpx.HTTPError as exc:
            log.warning("telegram alert failed: %s", exc)

    def reconcile_startup(self, now: datetime, config_hash: str) -> tuple[list[int], bool]:
        ttl = float(self.raw["telegram"]["pending_ttl_sec"])
        cutoff = (now.astimezone(UTC) - timedelta(seconds=ttl)).isoformat()
        abandoned = self.store.abandon_expired_pending(cutoff)
        prev = self.store.kv_get("last_config_hash")
        changed = prev is not None and prev != config_hash
        if prev is None or changed:
            self.store.kv_set("last_config_hash", config_hash)
        return abandoned, changed
