"""Admin commands via Telegram getUpdates short polling."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from memebot.notify import Notifier
from memebot.store import Store

log = logging.getLogger("memebot.admin")

OFFSET_KEY = "tg_admin_offset"

HELP = "/status 运行状态\n/signals 最近信号\n/help 本清单"


def _fmt_dt(ts: str) -> str:
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return parsed.astimezone(UTC).strftime("%m-%d %H:%M UTC")
    except ValueError:
        return ts


class AdminHandler:
    def __init__(
        self,
        store: Store,
        raw: dict[str, Any],
        notifier: Notifier,
        admin_id: str,
    ) -> None:
        self.store = store
        self.raw = raw
        self.notifier = notifier
        self.admin_id = admin_id

    async def poll_once(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        raw_offset = self.store.kv_get(OFFSET_KEY)
        offset = int(raw_offset) + 1 if raw_offset else None
        updates = await self.notifier.get_updates(offset=offset, timeout=0)
        last_id: int | None = None
        for upd in updates:
            if not isinstance(upd, dict):
                continue
            uid = upd.get("update_id")
            if isinstance(uid, int):
                last_id = max(last_id, uid) if last_id is not None else uid
            msg = upd.get("message")
            if not isinstance(msg, dict):
                continue
            sender = (msg.get("from") or {}).get("id")
            if str(sender or "") != str(self.admin_id):
                continue
            text = msg.get("text")
            if not isinstance(text, str):
                continue
            reply = self._handle(text, now)
            if reply:
                await self.notifier.send_admin(reply)
        if last_id is not None:
            self.store.kv_set(OFFSET_KEY, str(last_id))

    def _handle(self, text: str, now: datetime) -> str | None:
        parts = text.strip().split()
        cmd = parts[0].lower().split("@")[0]
        if cmd == "/status":
            return self._status(now)
        if cmd == "/signals":
            return self._signals()
        return HELP

    def _status(self, now: datetime) -> str:
        day = now.date().isoformat()
        started = self.store.kv_get("started_at") or "—"
        last_ok = self.store.kv_get("heartbeat_at") or "—"
        lines = [
            "状态",
            f"实例 {self.store.kv_get('instance_id') or '—'}",
            f"启动 {_fmt_dt(started)}",
            f"心跳 {_fmt_dt(last_ok)}",
            (
                f"雷达 {self.store.get_step(day, 'radar_input')} · "
                f"待分级 {self.store.get_step(day, 'grade_input')}"
            ),
            (
                f"强 {self.store.get_step(day, 'pushed_strong')} · "
                f"弱 {self.store.get_step(day, 'pushed_weak')}"
            ),
            f"额度日 {self.store.cg_calls_today(day)} · 月 {self.store.month_cg_calls(day[:7])}",
            f"进行中升段 {len(self.store.list_open_legs())}",
        ]
        return "\n".join(lines)

    def _signals(self) -> str:
        rows = self.store.list_signals(limit=10)
        if not rows:
            return "暂无信号"
        lines = ["最近信号"]
        for row in reversed(rows):
            lines.append(
                f"#{row['id']} {row['grade']} {row['network']} "
                f"{_fmt_dt(str(row['created_at']))} {row['status']}"
            )
        return "\n".join(lines)
