"""Admin commands via Telegram getUpdates short polling (admin-only)."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from memebot.notify import Notifier
from memebot.store import Store

log = logging.getLogger("memebot.admin")

OFFSET_KEY = "tg_admin_offset"

HELP = (
    "/status 运行状态\n"
    "/funnel 今日漏斗\n"
    "/watch 在盯会话\n"
    "/signals 最近信号\n"
    "/report 立即出日报\n"
    "/help 本清单"
)

ReportFn = Callable[[datetime], Awaitable[None]]


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
        watcher: Any,
        report_fn: ReportFn,
    ) -> None:
        self.store = store
        self.raw = raw
        self.notifier = notifier
        self.watcher = watcher
        self.report_fn = report_fn

    def _admin_id(self) -> str:
        return str(self.raw["telegram"]["admin_id"])

    async def poll_once(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        raw_offset = self.store.kv_get(OFFSET_KEY)
        offset = int(raw_offset) + 1 if raw_offset else None
        updates = await self.notifier.get_updates(offset=offset, timeout=0)
        last_id = offset
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
            if str(sender or "") != self._admin_id():
                continue
            text = msg.get("text")
            if not isinstance(text, str):
                continue
            reply = await self._handle(text, now)
            if reply:
                await self.notifier.send_admin(reply)
        if last_id is not None:
            self.store.kv_set(OFFSET_KEY, str(last_id))

    async def _handle(self, text: str, now: datetime) -> str | None:
        parts = text.strip().split()
        cmd = parts[0].lower().split("@")[0]
        if cmd == "/status":
            return self._status(now)
        if cmd == "/funnel":
            return self._funnel(now)
        if cmd == "/watch":
            return self._watch(now)
        if cmd == "/signals":
            return self._signals()
        if cmd == "/report":
            await self.report_fn(now)
            return "日报已推送"
        return HELP

    def _status(self, now: datetime) -> str:
        day = now.date().isoformat()
        month = day[:7]
        started = self.store.kv_get("started_at") or "—"
        last_ok = self.store.kv_get("last_collection_ok_at") or "—"
        lines = [
            "状态",
            f"启动 {_fmt_dt(started)}",
            f"最近采集 {_fmt_dt(last_ok)}",
            f"在盯 {len(self.watcher.sessions)} · 悬挂 {len(self.store.hanging_watches())}",
            f"额度 日{self.store.daily_calls(day)} "
            f"(watch {self.store.kind_calls(day, 'watch')}) 月{self.store.month_credits(month)}",
        ]
        return "\n".join(lines)

    def _funnel(self, now: datetime) -> str:
        day = now.date().isoformat()
        rows: dict[str, list[tuple[str, int]]] = {}
        for row in self.store.funnel_day(day):
            rule = str(row["rule"])
            if ":" in rule:
                continue
            rows.setdefault(str(row["layer"]), []).append((rule, int(row["n"])))
        order = ("stream", "l0", "l1", "l2a", "l2b", "scoring", "watch")
        lines = [f"漏斗 {day}"]
        for layer in order:
            entries = sorted(rows.get(layer, []), key=lambda x: -x[1])
            if not entries:
                continue
            inp = next((n for r, n in entries if r == "_input"), None)
            passed = next((n for r, n in entries if r == "_passed"), None)
            head = f"{layer}: 入 {inp or 0}"
            if passed is not None:
                head += f" · 过 {passed}"
            lines.append(head)
            for rule, n in entries[:5]:
                if rule.startswith("_"):
                    continue
                lines.append(f"  {rule} {n}")
        return "\n".join(lines)

    def _watch(self, now: datetime) -> str:
        sessions = list(self.watcher.sessions.values())
        if not sessions:
            return "当前无在盯会话"
        lines = ["在盯会话"]
        for sess in sorted(sessions, key=lambda s: -s.score):
            symbol = (sess.features.get("symbol") or "?")
            elapsed = (now.astimezone(UTC) - sess.entered_at.astimezone(UTC)).total_seconds() / 60
            chg = ""
            if sess.baseline and sess.last_price:
                pct = (sess.last_price / sess.baseline - 1) * 100
                sign = "+" if pct >= 0 else ""
                chg = f" {sign}{pct:.1f}%"
            lines.append(
                f"{symbol} 分{sess.score:.2f} 盯{elapsed:.0f}min{chg}"
            )
        return "\n".join(lines)

    def _signals(self) -> str:
        rows = self.store.read(
            lambda c: c.execute(
                "SELECT * FROM signals ORDER BY id DESC LIMIT 10"
            ).fetchall()
        )
        if not rows:
            return "暂无信号"
        lines = ["最近信号"]
        for row in reversed(rows):
            feats: dict[str, Any] = {}
            raw_f = row["features_json"]
            if raw_f:
                try:
                    loaded = json.loads(str(raw_f))
                    if isinstance(loaded, dict):
                        feats = loaded
                except ValueError:
                    feats = {}
            symbol = feats.get("symbol") or "?"
            lines.append(
                f"#{row['id']} {symbol} {_fmt_dt(str(row['created_at']))} {row['status']}"
            )
        return "\n".join(lines)
