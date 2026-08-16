"""Leg lifecycle: open, high-price maintenance, drawdown and inactivity end."""

from __future__ import annotations

from datetime import datetime, timedelta

from memebot.store import Store


def spot_valid(price: float | None) -> bool:
    return price is not None and price > 0


def ended_by_drawdown(high_price: float | None, spot: float, end_drawdown_pct: float) -> bool:
    if high_price is None or high_price <= 0:
        return False
    return (high_price - spot) / high_price * 100 >= end_drawdown_pct


def ensure_open_leg(
    store: Store,
    network: str,
    token_address: str,
    *,
    spot: float | None,
    last_seen_at: str,
) -> int:
    row = store.get_open_leg(network, token_address)
    if row is not None:
        return int(row["id"])
    high = spot if spot_valid(spot) else None
    return store.open_leg(network, token_address, high_price=high, last_seen_at=last_seen_at)


def touch_leg(
    store: Store,
    leg_id: int,
    *,
    spot: float | None,
    last_seen_at: str,
    end_drawdown_pct: float,
) -> bool:
    """Update last_seen and high price. Return True if the leg ended this call."""
    row = store.read(lambda c: c.execute("SELECT * FROM legs WHERE id = ?", (leg_id,)).fetchone())
    if row is None or int(row["ended"]) != 0:
        return False
    high = row["high_price"]
    if not spot_valid(spot):
        store.update_leg(leg_id, last_seen_at=last_seen_at)
        return False
    assert spot is not None
    if high is None:
        store.update_leg(leg_id, high_price=spot, last_seen_at=last_seen_at)
        return False
    new_high = max(float(high), spot)
    ended = ended_by_drawdown(new_high, spot, end_drawdown_pct)
    store.update_leg(leg_id, high_price=new_high, last_seen_at=last_seen_at, ended=ended)
    return ended


def init_high_from_close(store: Store, leg_id: int, c_now: float) -> None:
    if not spot_valid(c_now):
        return
    row = store.read(lambda c: c.execute("SELECT * FROM legs WHERE id = ?", (leg_id,)).fetchone())
    if row is None or int(row["ended"]) != 0:
        return
    if row["high_price"] is not None:
        return
    store.update_leg(leg_id, high_price=c_now)


def expire_inactive(store: Store, now: datetime, max_inactive_h: float) -> list[int]:
    ended: list[int] = []
    limit = timedelta(hours=max_inactive_h)
    for row in store.list_open_legs():
        raw = str(row["last_seen_at"])
        try:
            seen = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if seen.tzinfo is None:
            continue
        if now - seen >= limit:
            store.update_leg(int(row["id"]), ended=True)
            ended.append(int(row["id"]))
    return ended
