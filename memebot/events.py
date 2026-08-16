"""In-process event bus with optional event_log persistence."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from memebot.store import Store

Handler = Callable[[str, dict[str, Any]], Awaitable[None] | None]


class EventBus:
    def __init__(self, store: Store | None = None) -> None:
        self.store = store
        self._subs: dict[str, list[Handler]] = {}

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._subs.setdefault(event_type, []).append(handler)

    async def publish(
        self, event_type: str, payload: dict[str, Any], *, persist: bool = True
    ) -> None:
        if persist and self.store is not None:
            self.store.insert_event(
                event_type,
                datetime.now(UTC).isoformat(),
                json.dumps(payload, ensure_ascii=True),
            )
        for handler in self._subs.get(event_type, []):
            result = handler(event_type, payload)
            if result is not None:
                await result
