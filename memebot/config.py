"""Load .env secrets and config.yaml; reject invalid configs at startup."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_B58 = frozenset("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
_EVM = re.compile(r"^0x[0-9a-fA-F]{40}$")
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR"})
_STREAM_SOURCES = frozenset({"megafilter", "new_pools"})
_WEIGHT_EPS = 1e-9
_TURNOVER_GATE_TO_HALF = 3
_MINUTES_PER_HOUR = 60


class ConfigError(Exception):
    def __init__(self, path: str, message: str) -> None:
        self.path = path
        super().__init__(f"{path}: {message}")


@dataclass(frozen=True)
class Secrets:
    coingecko_api_key: str
    telegram_bot_token: str
    stop_grace_period: float


@dataclass(frozen=True)
class AppConfig:
    raw: dict[str, Any]
    secrets: Secrets
    config_hash: str

    def get(self, path: str, default: Any = None) -> Any:
        cur: Any = self.raw
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(name, "missing secret")
    return value


_REQUIRED_PATHS = (
    "runtime.log_level",
    "runtime.http.timeout_sec",
    "runtime.http.connect_timeout_sec",
    "runtime.http.max_connections",
    "runtime.http.user_agent",
    "runtime.retry.max_attempts",
    "runtime.retry.backoff_base_sec",
    "runtime.retry.backoff_factor",
    "runtime.retry.backoff_max_sec",
    "runtime.rate_limit.max_requests_per_min",
    "runtime.shutdown.grace_sec",
    "runtime.health.heartbeat_interval_sec",
    "runtime.health.stale_after_sec",
    "runtime.health.clock_skew_warn_sec",
    "paths.db",
    "paths.backup_dir",
    "paths.lock_file",
    "paths.heartbeat_file",
    "networks",
    "streams.source",
    "streams.megafilter.enabled",
    "streams.megafilter.interval_sec",
    "streams.megafilter.pages",
    "streams.megafilter.prefilter.pool_created_hour_min",
    "streams.megafilter.prefilter.pool_created_hour_max",
    "streams.megafilter.prefilter.reserve_in_usd_min",
    "streams.megafilter.prefilter.sort",
    "streams.rising.enabled",
    "streams.rising.interval_sec",
    "streams.rising.pages",
    "streams.rising.prefilter.sort",
    "streams.rising.prefilter.price_change_percentage_min",
    "streams.rising.prefilter.price_change_percentage_duration",
    "streams.rising.max_fdv_usd",
    "streams.rising.max_age_h",
    "streams.new_pools.enabled",
    "streams.new_pools.interval_sec",
    "streams.new_pools.pages",
    "streams.new_pools.maturation_queue.enabled",
    "streams.new_pools.maturation_queue.recheck_batch_size",
    "streams.new_pools.maturation_queue.max_queue_size",
    "streams.trending_5m.enabled",
    "streams.trending_5m.interval_sec",
    "streams.trending_5m.duration",
    "streams.trending_5m.pages",
    "streams.trending_5m.max_fdv_usd",
    "streams.trending_5m.max_age_h",
    "streams.trending_1h.enabled",
    "streams.trending_1h.interval_sec",
    "streams.trending_1h.duration",
    "streams.trending_1h.pages",
    "streams.trending_1h.max_fdv_usd",
    "streams.trending_1h.max_age_h",
    "collection_gates.quote_tokens",
    "collection_gates.min_reserve_usd",
    "collection_gates.max_fdv_usd",
    "collection_gates.max_fdv_to_reserve",
    "collection_gates.max_turnover_ratio.window",
    "collection_gates.max_turnover_ratio.max",
    "collection_gates.anti_wash.window",
    "collection_gates.anti_wash.min_buyers_to_buys",
    "collection_gates.anti_wash.min_buyers_m15",
    "collection_gates.copycat.enabled",
    "collection_gates.copycat.lookback_h",
    "collection_gates.copycat.max_same_symbol",
    "collection_gates.max_sus_reports",
    "business_gates.min_age_min",
    "business_gates.max_age_h",
    "business_gates.min_volume.window",
    "business_gates.min_volume.usd",
    "security.batch.enabled",
    "security.batch.size",
    "security.batch.min_main_pool_share",
    "security.batch.cache.graduated_ttl_min",
    "security.batch.cache.ungraduated_recheck_min",
    "security.batch.cache.main_pool_share_ttl_min",
    "security.cache_hours",
    "security.honeypot_policy",
    "security.reject_ungraduated",
    "security.holders_max_staleness_h",
    "security.unknown_policy.dev_holding",
    "security.unknown_policy.top10_holding",
    "security.unknown_policy.holders_count",
    "security.rules.solana",
    "security.rules.bsc",
    "scoring.combine",
    "scoring.momentum.weight",
    "scoring.momentum.use_native_price",
    "scoring.momentum.penalty_above_pct",
    "scoring.momentum.windows",
    "scoring.buy_pressure.weight",
    "scoring.buy_pressure.window",
    "scoring.turnover.weight",
    "scoring.turnover.window",
    "scoring.turnover.normalize.type",
    "scoring.turnover.normalize.half",
    "scoring.turnover.penalty_above",
    "scoring.freshness.weight",
    "scoring.freshness.normalize.type",
    "scoring.freshness.normalize.half_life_min",
    "scoring.min_admit_score",
    "scoring.candidates_per_chain_per_cycle",
    "watch.poll_interval_sec",
    "watch.max_concurrent",
    "watch.min_dwell_sec",
    "watch.window_min",
    "watch.min_trade_usd",
    "watch.admit.min_m5_pct",
    "watch.admit.min_m5_pct_on_red_m15",
    "watch.admit.min_m15_pct",
    "watch.confirm.confirm_min_buyers",
    "watch.confirm.min_buyer_seller_ratio",
    "watch.confirm.min_buy_sell_ratio",
    "watch.confirm.min_price_change_pct",
    "watch.confirm.max_drawdown_from_peak_pct",
    "watch.confirm.sane_pct_min",
    "watch.confirm.sane_pct_max",
    "watch.timeout_cooldown.max_timeouts",
    "watch.timeout_cooldown.cooldown_h",
    "watch.full_policy",
    "watch.daily_call_cap",
    "tracking.enabled",
    "tracking.track_negatives",
    "tracking.scan_interval_h",
    "tracking.horizons",
    "tracking.rug.reserve_drop_pct",
    "tracking.rug.price_drawdown_pct",
    "tracking.rug.confirm_hours",
    "budget.global_daily_call_cap",
    "budget.monthly_credit_warn_pct",
    "telegram.channel_id",
    "telegram.admin_id",
    "telegram.pending_ttl_sec",
    "telegram.max_send_failures",
    "telegram.send_timeout_sec",
    "telegram.alert_silence_sec",
    "telegram.consecutive_failure_alert",
    "telegram.symbol_max_len",
    "telegram.admin_poll_interval_sec",
    "storage.pools_retain_h",
    "storage.watch_log_retain_days",
    "storage.outcomes_raw_retain_days",
    "storage.credit_usage_retain_days",
    "storage.event_log_retain_days",
    "storage.cleanup_interval_h",
    "storage.vacuum_interval_h",
    "storage.wal_autocheckpoint_pages",
    "storage.wal_truncate_interval_h",
    "report.enabled",
    "report.daily_at_utc",
    "report.zero_signal_alert_days",
)


_ADMIN_PLACEHOLDERS = frozenset({"", "12345", "your_admin_id"})
_CHANNEL_PLACEHOLDERS = frozenset({"", "-100xxx", "your_channel_id"})


def _live_telegram_id(env_name: str, yaml_value: Any, placeholders: frozenset[str]) -> str:
    env = os.environ.get(env_name, "").strip()
    if env:
        return env
    val = str(yaml_value or "").strip()
    if val in placeholders or "xxx" in val.lower():
        raise ConfigError(
            env_name,
            f"set {env_name} in .env or a real telegram id in config.yaml",
        )
    return val


def apply_telegram_env(raw: dict[str, Any]) -> None:
    tg = raw.setdefault("telegram", {})
    if not isinstance(tg, dict):
        raise ConfigError("telegram", "must be a mapping")
    tg["admin_id"] = _live_telegram_id(
        "TELEGRAM_ADMIN_ID", tg.get("admin_id"), _ADMIN_PLACEHOLDERS
    )
    tg["channel_id"] = _live_telegram_id(
        "TELEGRAM_CHANNEL_ID", tg.get("channel_id"), _CHANNEL_PLACEHOLDERS
    )


def load_secrets() -> Secrets:
    raw_grace = os.environ.get("STOP_GRACE_PERIOD", "").strip()
    if not raw_grace:
        raise ConfigError("STOP_GRACE_PERIOD", "missing")
    try:
        grace = float(raw_grace)
    except ValueError as exc:
        raise ConfigError("STOP_GRACE_PERIOD", "must be a number") from exc
    return Secrets(
        coingecko_api_key=_require_env("COINGECKO_API_KEY"),
        telegram_bot_token=_require_env("TELEGRAM_BOT_TOKEN"),
        stop_grace_period=grace,
    )


def config_hash(raw: dict[str, Any]) -> str:
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:8]


def _need(raw: Any, path: str) -> Any:
    cur: Any = raw
    parts = path.split(".")
    for i, part in enumerate(parts):
        if not isinstance(cur, dict) or part not in cur:
            raise ConfigError(".".join(parts[: i + 1]), "missing required field")
        cur = cur[part]
    return cur


def _as_number(path: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(path, "must be a number")
    return float(value)


def _as_str(path: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(path, "must be a non-empty string")
    return value


def _is_solana_address(addr: str) -> bool:
    return 32 <= len(addr) <= 44 and all(c in _B58 for c in addr)


def _is_evm_address(addr: str) -> bool:
    return _EVM.fullmatch(addr) is not None


def _validate_quote_tokens(raw: dict[str, Any]) -> None:
    quotes = _need(raw, "collection_gates.quote_tokens")
    if not isinstance(quotes, dict):
        raise ConfigError("collection_gates.quote_tokens", "must be a mapping by network")
    networks = _need(raw, "networks")
    if not isinstance(networks, list) or not networks:
        raise ConfigError("networks", "must be a non-empty list")
    for net in networks:
        addrs = quotes.get(net)
        if not isinstance(addrs, list) or not addrs:
            raise ConfigError(
                f"collection_gates.quote_tokens.{net}",
                "must be a non-empty address list",
            )
        for i, addr in enumerate(addrs):
            path = f"collection_gates.quote_tokens.{net}[{i}]"
            if not isinstance(addr, str):
                raise ConfigError(path, "must be a contract address string")
            if net == "solana":
                if not _is_solana_address(addr):
                    raise ConfigError(
                        path,
                        "must be a Solana base58 contract address, not a symbol",
                    )
            else:
                if not _is_evm_address(addr):
                    raise ConfigError(
                        path,
                        "must be an EVM 0x+40hex contract address, not a symbol",
                    )


def validate(raw: dict[str, Any], stop_grace_period: float) -> None:
    for path in _REQUIRED_PATHS:
        _need(raw, path)
    level = _as_str("runtime.log_level", _need(raw, "runtime.log_level"))
    if level not in _LOG_LEVELS:
        raise ConfigError("runtime.log_level", f"must be one of {sorted(_LOG_LEVELS)}")

    weights = (
        _as_number("scoring.momentum.weight", _need(raw, "scoring.momentum.weight"))
        + _as_number("scoring.buy_pressure.weight", _need(raw, "scoring.buy_pressure.weight"))
        + _as_number("scoring.turnover.weight", _need(raw, "scoring.turnover.weight"))
        + _as_number("scoring.freshness.weight", _need(raw, "scoring.freshness.weight"))
    )
    if abs(weights - 1.0) > _WEIGHT_EPS:
        raise ConfigError("scoring", "four dimension weights must sum to 1.0")

    g = _as_number(
        "collection_gates.max_turnover_ratio.max",
        _need(raw, "collection_gates.max_turnover_ratio.max"),
    )
    half = _as_number(
        "scoring.turnover.normalize.half",
        _need(raw, "scoring.turnover.normalize.half"),
    )
    if g > _TURNOVER_GATE_TO_HALF * half:
        raise ConfigError(
            "collection_gates.max_turnover_ratio.max",
            "must be <= 3 * scoring.turnover.normalize.half",
        )

    pre_age_h = _as_number(
        "streams.megafilter.prefilter.pool_created_hour_min",
        _need(raw, "streams.megafilter.prefilter.pool_created_hour_min"),
    )
    min_age = _as_number("business_gates.min_age_min", _need(raw, "business_gates.min_age_min"))
    if pre_age_h * _MINUTES_PER_HOUR > min_age:
        raise ConfigError(
            "streams.megafilter.prefilter.pool_created_hour_min",
            "pool_created_hour_min * 60 must be <= business_gates.min_age_min",
        )
    pre_age_max = _as_number(
        "streams.megafilter.prefilter.pool_created_hour_max",
        _need(raw, "streams.megafilter.prefilter.pool_created_hour_max"),
    )
    max_age_h = _as_number("business_gates.max_age_h", _need(raw, "business_gates.max_age_h"))
    if pre_age_max < max_age_h:
        raise ConfigError(
            "streams.megafilter.prefilter.pool_created_hour_max",
            "must be >= business_gates.max_age_h",
        )
    pre_reserve = _as_number(
        "streams.megafilter.prefilter.reserve_in_usd_min",
        _need(raw, "streams.megafilter.prefilter.reserve_in_usd_min"),
    )
    min_reserve = _as_number(
        "collection_gates.min_reserve_usd",
        _need(raw, "collection_gates.min_reserve_usd"),
    )
    if pre_reserve > min_reserve:
        raise ConfigError(
            "streams.megafilter.prefilter.reserve_in_usd_min",
            "must be <= collection_gates.min_reserve_usd",
        )

    grace = _as_number("runtime.shutdown.grace_sec", _need(raw, "runtime.shutdown.grace_sec"))
    if grace >= stop_grace_period:
        raise ConfigError(
            "runtime.shutdown.grace_sec",
            "must be < STOP_GRACE_PERIOD",
        )

    source = _as_str("streams.source", _need(raw, "streams.source"))
    if source not in _STREAM_SOURCES:
        raise ConfigError("streams.source", "must be megafilter or new_pools")
    enabled = _need(raw, f"streams.{source}.enabled")
    if enabled is not True:
        raise ConfigError(f"streams.{source}.enabled", "source stream must be enabled")

    _validate_quote_tokens(raw)

    watch_cap = _as_number("watch.daily_call_cap", _need(raw, "watch.daily_call_cap"))
    global_cap = _as_number(
        "budget.global_daily_call_cap",
        _need(raw, "budget.global_daily_call_cap"),
    )
    if watch_cap >= global_cap:
        raise ConfigError("watch.daily_call_cap", "must be < budget.global_daily_call_cap")

    admit_m5 = _need(raw, "watch.admit.min_m5_pct")
    if admit_m5 is not None:
        _as_number("watch.admit.min_m5_pct", admit_m5)
    admit_m5_red = _need(raw, "watch.admit.min_m5_pct_on_red_m15")
    if admit_m5_red is not None:
        _as_number("watch.admit.min_m5_pct_on_red_m15", admit_m5_red)
    admit_m15 = _need(raw, "watch.admit.min_m15_pct")
    if admit_m15 is not None:
        _as_number("watch.admit.min_m15_pct", admit_m15)

    dd = _need(raw, "watch.confirm.max_drawdown_from_peak_pct")
    if dd is not None:
        dd_n = _as_number("watch.confirm.max_drawdown_from_peak_pct", dd)
        if dd_n <= 0:
            raise ConfigError("watch.confirm.max_drawdown_from_peak_pct", "must be > 0")

    pages = _as_number("streams.megafilter.pages", _need(raw, "streams.megafilter.pages"))
    if pages < 1 or int(pages) != pages:
        raise ConfigError("streams.megafilter.pages", "must be an integer >= 1")

    min_chg = _as_number(
        "watch.confirm.min_price_change_pct",
        _need(raw, "watch.confirm.min_price_change_pct"),
    )
    sane_min = _need(raw, "watch.confirm.sane_pct_min")
    sane_max = _need(raw, "watch.confirm.sane_pct_max")
    lo = _as_number("watch.confirm.sane_pct_min", sane_min) if sane_min is not None else None
    hi = _as_number("watch.confirm.sane_pct_max", sane_max) if sane_max is not None else None
    if lo is not None and hi is not None and lo >= hi:
        raise ConfigError("watch.confirm.sane_pct_min", "must be < watch.confirm.sane_pct_max")
    if lo is not None and min_chg < lo:
        raise ConfigError(
            "watch.confirm.min_price_change_pct",
            "must be >= watch.confirm.sane_pct_min",
        )
    if hi is not None and min_chg > hi:
        raise ConfigError(
            "watch.confirm.min_price_change_pct",
            "must be <= watch.confirm.sane_pct_max",
        )

    base_fdv = _need(raw, "collection_gates.max_fdv_usd")
    base_age = _need(raw, "business_gates.max_age_h")
    for name in ("trending_5m", "trending_1h", "rising"):
        fdv_path = f"streams.{name}.max_fdv_usd"
        age_path = f"streams.{name}.max_age_h"
        ov_fdv = _need(raw, fdv_path)
        ov_age = _need(raw, age_path)
        if ov_fdv is not None:
            n_fdv = _as_number(fdv_path, ov_fdv)
            if base_fdv is not None:
                base = _as_number("collection_gates.max_fdv_usd", base_fdv)
                if n_fdv < base:
                    raise ConfigError(fdv_path, "must be >= collection_gates.max_fdv_usd")
        if ov_age is not None:
            n_age = _as_number(age_path, ov_age)
            if base_age is not None and n_age < _as_number("business_gates.max_age_h", base_age):
                raise ConfigError(age_path, "must be >= business_gates.max_age_h")

    rising_enabled = _need(raw, "streams.rising.enabled")
    if rising_enabled is True:
        rising_sort = _as_str(
            "streams.rising.prefilter.sort", _need(raw, "streams.rising.prefilter.sort")
        )
        allowed_sorts = frozenset(
            f"{w}_price_change_percentage_desc" for w in ("m5", "h1", "h6", "h24")
        )
        if rising_sort not in allowed_sorts:
            raise ConfigError(
                "streams.rising.prefilter.sort",
                f"must be one of {sorted(allowed_sorts)}",
            )
        rising_dur = _as_str(
            "streams.rising.prefilter.price_change_percentage_duration",
            _need(raw, "streams.rising.prefilter.price_change_percentage_duration"),
        )
        if rising_dur not in {"5m", "1h", "6h", "24h"}:
            raise ConfigError(
                "streams.rising.prefilter.price_change_percentage_duration",
                "must be one of 5m, 1h, 6h, 24h",
            )
        rising_min = _as_number(
            "streams.rising.prefilter.price_change_percentage_min",
            _need(raw, "streams.rising.prefilter.price_change_percentage_min"),
        )
        if rising_min <= 0:
            raise ConfigError(
                "streams.rising.prefilter.price_change_percentage_min", "must be > 0"
            )

    min_score = _need(raw, "scoring.min_admit_score")
    if min_score is not None:
        score_n = _as_number("scoring.min_admit_score", min_score)
        if score_n < 0 or score_n > 1:
            raise ConfigError("scoring.min_admit_score", "must be between 0 and 1")

    horizons = _need(raw, "tracking.horizons")
    if not isinstance(horizons, list) or not horizons:
        raise ConfigError("tracking.horizons", "must be a non-empty list")
    for i, horizon in enumerate(horizons):
        path = f"tracking.horizons[{i}]"
        if not isinstance(horizon, dict):
            raise ConfigError(path, "must be a mapping")
        after = horizon.get("after_h")
        if not isinstance(after, int) or isinstance(after, bool) or after < 1:
            raise ConfigError(f"{path}.after_h", "must be an integer >= 1")
        if str(horizon.get("tf") or "") not in {"minute", "hour", "day"}:
            raise ConfigError(f"{path}.tf", "must be minute, hour or day")
        agg = horizon.get("agg")
        if not isinstance(agg, int) or isinstance(agg, bool) or agg < 1:
            raise ConfigError(f"{path}.agg", "must be an integer >= 1")
        limit = horizon.get("limit")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > 1000:
            raise ConfigError(f"{path}.limit", "must be an integer between 1 and 1000")


def load_config(config_path: Path, env_path: Path | None = None) -> AppConfig:
    if env_path is not None:
        load_dotenv(env_path)
    elif Path(".env").is_file():
        load_dotenv(Path(".env"))
    secrets = load_secrets()
    if not config_path.is_file():
        raise ConfigError(str(config_path), "config file not found")
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ConfigError(str(config_path), "root must be a mapping")
    apply_telegram_env(loaded)
    validate(loaded, secrets.stop_grace_period)
    return AppConfig(raw=loaded, secrets=secrets, config_hash=config_hash(loaded))
