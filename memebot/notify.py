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
        "reserve_usd": feats.get("reserve_usd"),
        "age_min": feats.get("age_min"),
        "holders": feats.get("holders"),
        "buyers": feats.get("buyers"),
        "sellers": feats.get("sellers"),
        "buy_sell_ratio": feats.get("buy_sell_ratio"),
        "price_change_pct": feats.get("price_change_pct"),
        "price_change_usd": feats.get("price_change_usd"),
        "price_change_native": feats.get("price_change_native"),
    }


def sanitize_chain_text(value: str, max_len: int) -> str:
    text = html.escape(value, quote=True)
    text = _ZW.sub("", text)
    text = _CTRL.sub("", text)
    return text[:max_len]


def render_signal(payload: dict[str, Any], raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    max_len = int(raw["telegram"]["symbol_max_len"])
    symbol = sanitize_chain_text(str(payload.get("symbol") or "?"), max_len)
    network = str(payload.get("network") or "")
    ca = html.escape(str(payload.get("token_address") or ""), quote=True)
    created = str(payload.get("created_at") or "")
    honeypot = "未检测" if network == "solana" else str(payload.get("honeypot") or "未知")
    age = payload.get("age_min")
    age_s = f"{float(age):.1f}min" if age is not None else "?"
    raw_chg = payload.get("price_change_usd")
    chg = raw_chg if isinstance(raw_chg, dict) else {}
    chg_s = (
        f"m5 {chg.get('m5', '?')}% / m15 {chg.get('m15', '?')}% / h1 {chg.get('h1', '?')}%"
        if chg
        else "m5 ?% / m15 ?% / h1 ?%"
    )
    text = (
        f"🚀 ${symbol} ({html.escape(network)}) · 池龄 {html.escape(str(age_s))}\n"
        f"CA: <code>{ca}</code>\n"
        f"FDV ${payload.get('fdv_usd') or '?'} · 流动性 ${payload.get('reserve_usd') or '?'}"
        f" · 持有人 {payload.get('holders') or '?'}\n"
        f"涨跌 {html.escape(chg_s)}\n"
        f"确认: 独立买家 {payload.get('buyers')}/{payload.get('sellers')} · "
        f"买卖比 {payload.get('buy_sell_ratio')} · {payload.get('price_change_pct')}%\n"
        f"安全: 蜜罐 {honeypot}\n"
        f"时间: {html.escape(created)} UTC"
    )
    net_q = quote(network)
    pool_q = quote(str(payload.get("pool_address") or payload.get("token_address") or ""))
    token_q = quote(str(payload.get("token_address") or ""))
    markup = {
        "inline_keyboard": [
            [
                {
                    "text": "GeckoTerminal",
                    "url": f"https://www.geckoterminal.com/{net_q}/pools/{pool_q}",
                },
                {"text": "DexScreener", "url": f"https://dexscreener.com/{net_q}/{token_q}"},
            ]
        ]
    }
    return text, markup


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

    async def _send(self, chat_id: str, text: str, markup: dict[str, Any] | None) -> None:
        try:
            await self._client.post(
                f"/bot{self.token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "reply_markup": markup,
                },
            )
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
