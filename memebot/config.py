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
DEPLOY_STOP_GRACE_SEC = 35.0

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
    "streams.momentum.enabled",
    "streams.momentum.interval_sec",
    "streams.momentum.pages",
    "streams.momentum.prefilter.sort",
    "streams.momentum.prefilter.price_change_percentage_min",
    "streams.momentum.prefilter.price_change_percentage_duration",
    "streams.momentum.prefilter.reserve_in_usd_min",
    "streams.momentum.prefilter.pool_created_hour_min",
    "streams.momentum.prefilter.pool_created_hour_max",
    "streams.trending_5m.enabled",
    "streams.trending_5m.interval_sec",
    "streams.trending_5m.pages",
    "streams.trending_1h.enabled",
    "streams.trending_1h.interval_sec",
    "streams.trending_1h.pages",
    "radar.min_m5_pct",
    "gates.min_pool_age_min",
    "gates.min_reserve_usd",
    "gates.min_fdv_usd",
    "gates.max_m15_vol_to_reserve",
    "gates.quote_tokens",
    "security.cache_ttl_min",
    "security.transient_ttl_sec",
    "security.max_tax_pct",
    "security.timeout_sec",
    "security.batch_size",
    "grade.ohlcv_limit",
    "grade.near_high_bars",
    "grade.weak_min_1m_pct",
    "grade.strong_min_1m_pct",
    "grade.strong_min_1m_to_h1",
    "grade.strong_min_1m_to_m5",
    "grade.strong_max_dist_pct",
    "grade.weak_max_dist_pct",
    "grade.trade_lookback_sec",
    "grade.min_trade_usd",
    "grade.require_net_buy",
    "legs.end_drawdown_pct",
    "legs.max_inactive_h",
    "tracker.after_h",
    "tracker.scan_interval_h",
    "tracker.drawdown_pct",
    "tracker.max_attempts",
    "budget.cg_daily_call_cap",
    "telegram.strong_channel_id",
    "telegram.weak_channel_id",
    "telegram.pending_ttl_sec",
    "telegram.max_send_failures",
    "telegram.send_timeout_sec",
    "telegram.alert_silence_sec",
    "telegram.symbol_max_len",
    "telegram.admin_poll_interval_sec",
    "storage.wal_autocheckpoint_pages",
    "storage.cleanup_interval_h",
    "storage.outcomes_retain_days",
    "storage.event_log_retain_days",
    "storage.legs_retain_days",
)

_ALLOWED_TOP = frozenset(
    {
        "networks",
        "runtime",
        "paths",
        "streams",
        "radar",
        "gates",
        "security",
        "grade",
        "legs",
        "tracker",
        "budget",
        "telegram",
        "storage",
    }
)


class ConfigError(Exception):
    def __init__(self, path: str, message: str) -> None:
        self.path = path
        super().__init__(f"{path}: {message}")


@dataclass(frozen=True)
class Secrets:
    coingecko_api_key: str
    goplus_app_key: str
    goplus_app_secret: str
    telegram_bot_token: str
    telegram_admin_id: str


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


def load_secrets() -> Secrets:
    return Secrets(
        coingecko_api_key=_require_env("COINGECKO_API_KEY"),
        goplus_app_key=_require_env("GOPLUS_APP_KEY"),
        goplus_app_secret=_require_env("GOPLUS_APP_SECRET"),
        telegram_bot_token=_require_env("TELEGRAM_BOT_TOKEN"),
        telegram_admin_id=_require_env("TELEGRAM_ADMIN_ID"),
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
    if isinstance(value, bool) or not isinstance(value, int | float):
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


def _validate_channel_id(path: str, value: Any) -> str:
    text = _as_str(path, value)
    lowered = text.lower()
    if "xxx" in lowered or text == "..." or "..." in text:
        raise ConfigError(path, "must not be a placeholder")
    return text


def _validate_quote_tokens(raw: dict[str, Any]) -> None:
    quotes = _need(raw, "gates.quote_tokens")
    if not isinstance(quotes, dict):
        raise ConfigError("gates.quote_tokens", "must be a mapping by network")
    networks = _need(raw, "networks")
    if not isinstance(networks, list) or not networks:
        raise ConfigError("networks", "must be a non-empty list")
    for net in networks:
        if not isinstance(net, str) or not net:
            raise ConfigError("networks", "must be a non-empty list of strings")
        addrs = quotes.get(net)
        if not isinstance(addrs, list) or not addrs:
            raise ConfigError(f"gates.quote_tokens.{net}", "must be a non-empty address list")
        for i, addr in enumerate(addrs):
            path = f"gates.quote_tokens.{net}[{i}]"
            if not isinstance(addr, str):
                raise ConfigError(path, "must be a contract address string")
            if net == "solana":
                if not _is_solana_address(addr):
                    raise ConfigError(path, "must be a Solana base58 contract address")
            else:
                if not _is_evm_address(addr):
                    raise ConfigError(path, "must be an EVM 0x+40hex contract address")


def _optional_number(raw: dict[str, Any], path: str) -> float | None:
    value = _need(raw, path)
    if value is None:
        return None
    return _as_number(path, value)


def validate(raw: dict[str, Any], *, stop_grace_period: float = DEPLOY_STOP_GRACE_SEC) -> None:
    extra = set(raw) - _ALLOWED_TOP
    if extra:
        raise ConfigError(sorted(extra)[0], "unknown config key")
    for path in _REQUIRED_PATHS:
        _need(raw, path)

    level = _as_str("runtime.log_level", _need(raw, "runtime.log_level"))
    if level not in _LOG_LEVELS:
        raise ConfigError("runtime.log_level", f"must be one of {sorted(_LOG_LEVELS)}")

    networks = _need(raw, "networks")
    if not isinstance(networks, list) or not networks:
        raise ConfigError("networks", "must be a non-empty list")

    _validate_quote_tokens(raw)

    strong_1m = _as_number("grade.strong_min_1m_pct", _need(raw, "grade.strong_min_1m_pct"))
    weak_1m = _as_number("grade.weak_min_1m_pct", _need(raw, "grade.weak_min_1m_pct"))
    if strong_1m < weak_1m:
        raise ConfigError("grade.strong_min_1m_pct", "must be >= grade.weak_min_1m_pct")

    strong_dist = _as_number(
        "grade.strong_max_dist_pct", _need(raw, "grade.strong_max_dist_pct")
    )
    weak_dist = _as_number("grade.weak_max_dist_pct", _need(raw, "grade.weak_max_dist_pct"))
    if strong_dist > weak_dist:
        raise ConfigError("grade.strong_max_dist_pct", "must be <= grade.weak_max_dist_pct")

    h1_ratio = _as_number("grade.strong_min_1m_to_h1", _need(raw, "grade.strong_min_1m_to_h1"))
    if h1_ratio <= 0:
        raise ConfigError("grade.strong_min_1m_to_h1", "must be > 0")
    m5_ratio = _as_number("grade.strong_min_1m_to_m5", _need(raw, "grade.strong_min_1m_to_m5"))
    if m5_ratio <= 0:
        raise ConfigError("grade.strong_min_1m_to_m5", "must be > 0")

    lookback = _as_number("grade.trade_lookback_sec", _need(raw, "grade.trade_lookback_sec"))
    if lookback <= 0:
        raise ConfigError("grade.trade_lookback_sec", "must be > 0")
    min_trade = _as_number("grade.min_trade_usd", _need(raw, "grade.min_trade_usd"))
    if min_trade < 0:
        raise ConfigError("grade.min_trade_usd", "must be >= 0")

    require_net = _need(raw, "grade.require_net_buy")
    if not isinstance(require_net, bool):
        raise ConfigError("grade.require_net_buy", "must be a boolean")

    min_m5 = _optional_number(raw, "radar.min_m5_pct")
    pre_m5 = _as_number(
        "streams.momentum.prefilter.price_change_percentage_min",
        _need(raw, "streams.momentum.prefilter.price_change_percentage_min"),
    )
    if min_m5 is not None and min_m5 < pre_m5:
        raise ConfigError(
            "radar.min_m5_pct",
            "must be >= streams.momentum.prefilter.price_change_percentage_min",
        )

    min_age = _optional_number(raw, "gates.min_pool_age_min")
    if min_age is not None and min_age < 0:
        raise ConfigError("gates.min_pool_age_min", "must be >= 0")
    min_fdv = _optional_number(raw, "gates.min_fdv_usd")
    if min_fdv is not None and min_fdv < 0:
        raise ConfigError("gates.min_fdv_usd", "must be >= 0")
    max_turn = _optional_number(raw, "gates.max_m15_vol_to_reserve")
    if max_turn is not None and max_turn <= 0:
        raise ConfigError("gates.max_m15_vol_to_reserve", "must be > 0")

    transient = _as_number(
        "security.transient_ttl_sec", _need(raw, "security.transient_ttl_sec")
    )
    cache_min = _as_number("security.cache_ttl_min", _need(raw, "security.cache_ttl_min"))
    if transient >= cache_min * 60:
        raise ConfigError(
            "security.transient_ttl_sec",
            "must be < security.cache_ttl_min × 60",
        )

    grace = _as_number("runtime.shutdown.grace_sec", _need(raw, "runtime.shutdown.grace_sec"))
    if grace >= stop_grace_period:
        raise ConfigError(
            "runtime.shutdown.grace_sec",
            f"must be < deploy stop_grace_period ({stop_grace_period:g}s)",
        )

    inactive = _as_number("legs.max_inactive_h", _need(raw, "legs.max_inactive_h"))
    if inactive <= 0:
        raise ConfigError("legs.max_inactive_h", "must be > 0")

    strong_ch = _validate_channel_id(
        "telegram.strong_channel_id", _need(raw, "telegram.strong_channel_id")
    )
    weak_ch = _validate_channel_id(
        "telegram.weak_channel_id", _need(raw, "telegram.weak_channel_id")
    )
    if strong_ch == weak_ch:
        raise ConfigError("telegram.weak_channel_id", "must differ from strong_channel_id")

    for name in ("momentum", "trending_5m", "trending_1h"):
        enabled = _need(raw, f"streams.{name}.enabled")
        if not isinstance(enabled, bool):
            raise ConfigError(f"streams.{name}.enabled", "must be a boolean")
        pages = _as_number(f"streams.{name}.pages", _need(raw, f"streams.{name}.pages"))
        if pages < 1 or int(pages) != pages:
            raise ConfigError(f"streams.{name}.pages", "must be an integer >= 1")


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
    validate(loaded)
    return AppConfig(raw=loaded, secrets=secrets, config_hash=config_hash(loaded))
