"""Post-hoc OHLCV metrics and rug flag (price path only)."""

from __future__ import annotations

import json
import statistics
from datetime import UTC, datetime, timedelta
from typing import Any

from memebot.store import Store


def ohlcv_rows(payload: dict[str, Any]) -> list[list[float]]:
    data = payload.get("data")
    attrs = data.get("attributes") if isinstance(data, dict) else {}
    rows = attrs.get("ohlcv_list") if isinstance(attrs, dict) else []
    out: list[list[float]] = []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, list) and len(row) >= 5:
                out.append([float(x) for x in row[:6]])
    out.sort(key=lambda r: r[0])
    return out


def report_date_utc(now: datetime) -> str:
    """Last completed UTC day — used when the daily report fires after midnight."""
    return (now.astimezone(UTC).date() - timedelta(days=1)).isoformat()


def compute_metrics(
    rows: list[list[float]],
    *,
    baseline: float,
    start_ts: datetime,
    drawdown_pct: float,
    confirm_hours: float,
) -> dict[str, Any]:
    if not rows or baseline <= 0:
        return {
            "max_gain_pct": None,
            "max_drawdown_pct": None,
            "t_to_peak_min": None,
            "price_1h": None,
            "price_24h": None,
            "is_rug": False,
        }
    peak = baseline
    peak_ts = start_ts
    max_gain = 0.0
    max_dd = 0.0
    price_1h = None
    price_24h = None
    for row in rows:
        ts = datetime.fromtimestamp(row[0], tz=UTC)
        high, low, close = row[2], row[3], row[4]
        gain = (high - baseline) / baseline * 100
        if gain > max_gain:
            max_gain = gain
            peak = high
            peak_ts = ts
        if peak > 0:
            max_dd = max(max_dd, (peak - low) / peak * 100)
        age_h = (ts - start_ts).total_seconds() / 3600
        if abs(age_h - 1) < 0.26:
            price_1h = close
        if abs(age_h - 24) < 0.4:
            price_24h = close
    t_to_peak = (peak_ts - start_ts).total_seconds() / 60
    return {
        "max_gain_pct": max_gain,
        "max_drawdown_pct": max_dd,
        "t_to_peak_min": t_to_peak,
        "price_1h": price_1h,
        "price_24h": price_24h,
        "is_rug": _rug(rows, start_ts, drawdown_pct, confirm_hours),
    }


def _rug(
    rows: list[list[float]], start: datetime, drawdown_pct: float, confirm_hours: float
) -> bool:
    peak = 0.0
    peak_ts: datetime | None = None
    for row in rows:
        ts = datetime.fromtimestamp(row[0], tz=UTC)
        high, close = row[2], row[4]
        if high >= peak:
            peak = high
            peak_ts = ts
        if peak_ts is None or peak <= 0:
            continue
        dd = (peak - close) / peak * 100
        if dd >= drawdown_pct and ts - peak_ts >= timedelta(hours=confirm_hours):
            return True
    return False


def build_daily_report(
    *,
    config_hash: str,
    sent: int,
    doubled_1h: int,
    rugs: int,
    outcomes_n: int,
    timeout_n: int,
    timeout_later_double: int,
    dwell_median: float | None,
    funnel: list[tuple[str, str, int]],
    top_l2_rule: str | None,
    day_credits: int,
    month_credits: int,
    zero_days: int,
    zero_alert_days: int,
    timeout_max_summary: str,
) -> str:
    hit = (doubled_1h / sent * 100) if sent else 0.0
    rug_rate = (rugs / sent * 100) if sent else 0.0
    fn_rate = (timeout_later_double / timeout_n * 100) if timeout_n else 0.0
    lines = [
        f"日报 {config_hash}",
        f"推送 {sent} · 1h翻倍 {hit:.1f}% · rug {rug_rate:.1f}% · 假阴 {fn_rate:.1f}%",
        f"超时 {timeout_n}（其后翻倍 {timeout_later_double}）· 驻留中位 {dwell_median}",
        f"额度 日{day_credits} 月{month_credits}",
    ]
    if top_l2_rule:
        lines.append(f"第2层贡献最大规则 {top_l2_rule}")
    for layer, rule, n in funnel:
        lines.append(f"漏斗 {layer}/{rule}={n}")
    if zero_days >= zero_alert_days:
        lines.append(f"⚠ 连续 {zero_days} 天零推送，确认条件可能过严")
        lines.append(timeout_max_summary)
    return "\n".join(lines)


def report_stats(store: Store, day: str) -> dict[str, Any]:
    sent = store.sent_count_on(day)
    signal_rows = store.read(
        lambda c: c.execute(
            "SELECT o.* FROM signal_outcomes o "
            "JOIN signals s ON o.source = 'signal' AND o.source_id = s.id "
            "WHERE s.created_at LIKE ?",
            (f"{day}%",),
        ).fetchall()
    )
    timeout_rows = store.read(
        lambda c: c.execute(
            "SELECT o.* FROM signal_outcomes o "
            "JOIN watch_log w ON o.source = 'watch_timeout' AND o.source_id = w.id "
            "WHERE w.started_at LIKE ?",
            (f"{day}%",),
        ).fetchall()
    )
    evicted_rows = store.read(
        lambda c: c.execute(
            "SELECT o.* FROM signal_outcomes o "
            "JOIN watch_log w ON o.source = 'watch_evicted' AND o.source_id = w.id "
            "WHERE w.started_at LIKE ?",
            (f"{day}%",),
        ).fetchall()
    )
    outcomes_n = len(signal_rows) + len(timeout_rows) + len(evicted_rows)
    doubled = 0
    rugs = 0
    for row in signal_rows:
        base = row["baseline_price"]
        p1 = row["price_1h"]
        if base and p1 is not None and float(p1) >= 2 * float(base):
            doubled += 1
        if int(row["is_rug"] or 0):
            rugs += 1
    later_double = 0
    for row in timeout_rows:
        if row["max_gain_pct"] is not None and float(row["max_gain_pct"]) >= 100:
            later_double += 1
    watches = store.read(
        lambda c: c.execute(
            "SELECT stats_json, outcome FROM watch_log WHERE started_at LIKE ?",
            (f"{day}%",),
        ).fetchall()
    )
    dwells: list[float] = []
    max_buyers = 0
    for row in watches:
        if not row["stats_json"]:
            continue
        stats = json.loads(str(row["stats_json"]))
        if stats.get("actual_dwell_sec") is not None:
            dwells.append(float(stats["actual_dwell_sec"]))
        if row["outcome"] == "timeout":
            max_buyers = max(max_buyers, int(stats.get("max_buyers") or 0))
    timeout_n = store.read(
        lambda c: c.execute(
            "SELECT COUNT(*) AS n FROM watch_log WHERE outcome = 'timeout' AND started_at LIKE ?",
            (f"{day}%",),
        ).fetchone()
    )
    return {
        "sent": sent,
        "doubled_1h": doubled,
        "rugs": rugs,
        "outcomes_n": outcomes_n,
        "timeout_n": int(timeout_n["n"]) if timeout_n else 0,
        "timeout_later_double": later_double,
        "dwell_median": statistics.median(dwells) if dwells else None,
        "timeout_max_summary": f"timeout max buyers={max_buyers}",
    }
