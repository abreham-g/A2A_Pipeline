"""Configuration loading for RocketSource automation."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List

from .errors import ConfigError


def _env(name: str) -> str | None:
    """Get an environment variable or return None if unset/blank."""
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return None
    return v


def _float_env(name: str, default: float) -> float:
    """Parse a float environment variable, falling back to default."""
    v = _env(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError as e:
        raise ConfigError(f"Invalid {name}: expected a number") from e


def _int_env(name: str, default: int) -> int:
    """Parse an integer environment variable, falling back to default."""
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError as e:
        raise ConfigError(f"Invalid {name}: expected an integer") from e


def _bool_env(name: str, default: bool) -> bool:
    """Parse a boolean environment variable, falling back to default."""
    v = _env(name)
    if v is None:
        return default
    v_lower = v.strip().lower()
    if v_lower in ("true", "yes", "1", "on"):
        return True
    if v_lower in ("false", "no", "0", "off"):
        return False
    raise ConfigError(f"Invalid {name}: expected a boolean value")


def _list_int_env(name: str, default: List[int]) -> List[int]:
    """Parse a comma-separated list of integers from environment variable."""
    v = _env(name)
    if v is None:
        return default
    try:
        return [int(item.strip()) for item in v.split(",") if item.strip()]
    except ValueError as e:
        raise ConfigError(f"Invalid {name}: expected comma-separated integers") from e


@dataclass(frozen=True)
class RocketSourceConfig:
    """Configuration for RocketSource API calls and polling."""
    base_url: str
    api_key: str

    api_key_header: str = "Authorization"
    api_key_prefix: str = "Bearer "

    upload_path: str = "/scans"
    upload_file_field: str = "file"

    scan_path: str = "/scans"
    scan_payload_template: str = '{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"UK","name":"Automated Scan"}}'

    status_path_template: str = "/scans/{scan_id}"
    results_path_template: str = "/scans/{scan_id}/download?type=csv"

    poll_interval_s: float = 3.0
    poll_timeout_s: float = 600.0

    log_level: str = "INFO"

    # Retry configuration
    max_retries: int = 3
    retry_delay: int = 30  # seconds between retries
    exponential_backoff: bool = True
    base_delay: int = 1  # Base delay for exponential backoff (seconds)
    
    # Active scan handling
    wait_for_active_scans: bool = True  # Wait for active scans to complete
    max_wait_time: int = 3600  # Max time to wait for active scan (seconds)
    
    # Optional: override default retry behavior for specific status codes
    retry_status_codes: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])

    # Database configuration
    db_url: str = ""
    db_connect_timeout_s: Optional[int] = None
    db_statement_timeout_ms: Optional[int] = None
    
    # Database table configuration - Updated with your environment variable defaults
    target_schema: str = "api_scraper"
    ungated_table: str = "test_tools_ungated"
    united_state_table: str = "United States"
    tirhak_schema: str = "gated"
    tirhak_table: str = "tirhak_gating_results_avg_tools_asins"
    umair_schema: str = "Core Data"
    umair_table: str = "umair_gating_results_tools"
    
    # Database performance settings
    db_batch_size: int = 1000
    db_enable_logging: bool = True
    
    # Sharding configuration
    shard_field: str = "avg90_SALES"
    shard_size: int = 10000
    paging_order: str = "oldest"
    snapshot_freeze: bool = True
    
    # Rate limiting configuration
    reqs_per_minute: int = 60
    reqs_per_hour: int = 3600
    tokens_reserve: int = 50
    tokens_poll_sec: int = 10

    @property
    def project_root(self) -> Path:
        """Project root directory (folder containing Data/ and Script/)."""
        return Path(__file__).resolve().parents[1]

    @property
    def data_dir(self) -> Path:
        """Default directory for input/output CSV files."""
        return self.project_root / "Data"

    @classmethod
    def from_env(cls) -> "RocketSourceConfig":
        """Create a config instance from environment variables."""
        base_url = _env("ROCKETSOURCE_BASE_URL")
        api_key = _env("ROCKETSOURCE_API_KEY") or _env("API_KEY")
        if not base_url:
            raise ConfigError("Missing ROCKETSOURCE_BASE_URL")
        if not api_key:
            raise ConfigError("Missing ROCKETSOURCE_API_KEY (or API_KEY)")

        # Get retry configuration with defaults
        max_retries = _int_env("ROCKETSOURCE_MAX_RETRIES", 3)
        retry_delay = _int_env("ROCKETSOURCE_RETRY_DELAY", 30)
        exponential_backoff = _bool_env("ROCKETSOURCE_EXPONENTIAL_BACKOFF", True)
        base_delay = _int_env("ROCKETSOURCE_BASE_DELAY", 1)
        
        # Get active scan handling configuration
        wait_for_active_scans = _bool_env("ROCKETSOURCE_WAIT_FOR_ACTIVE_SCANS", True)
        max_wait_time = _int_env("ROCKETSOURCE_MAX_WAIT_TIME", 3600)
        
        # Get retry status codes
        retry_status_codes = _list_int_env("ROCKETSOURCE_RETRY_STATUS_CODES", [429, 500, 502, 503, 504])

        # Get database configuration
        db_url = _env("DATABASE_URL") or _env("ROCKETSOURCE_DB_URL") or ""
        db_connect_timeout_s = _int_env("ROCKETSOURCE_DB_CONNECT_TIMEOUT_S", 10)
        db_statement_timeout_ms = _int_env("ROCKETSOURCE_DB_STATEMENT_TIMEOUT_MS", 60000)
        
        # Get database table configuration - Using your environment variables as defaults
        target_schema = _env("ROCKETSOURCE_TARGET_SCHEMA") or "api_scraper"
        ungated_table = _env("ROCKETSOURCE_UNGATED_TABLE") or "test_tools_ungated"
        united_state_table = _env("ROCKETSOURCE_UNITED_STATE_TABLE") or "United States"
        tirhak_schema = _env("ROCKETSOURCE_TIRHAK_SCHEMA") or "gated"
        tirhak_table = _env("ROCKETSOURCE_TIRHAK_TABLE") or "tirhak_gating_results_avg_tools_asins"
        umair_schema = _env("ROCKETSOURCE_UMAIR_SCHEMA") or "Core Data"
        umair_table = _env("ROCKETSOURCE_UMAIR_TABLE") or "umair_gating_results_tools"
        
        # Get database performance settings
        db_batch_size = _int_env("ROCKETSOURCE_DB_BATCH_SIZE", 1000)
        db_enable_logging = _bool_env("ROCKETSOURCE_DB_ENABLE_LOGGING", True)
        
        # Get sharding configuration
        shard_field = _env("SHARD_FIELD") or "avg90_SALES"
        shard_size = _int_env("SHARD_SIZE", 10000)
        paging_order = _env("PAGING_ORDER") or "oldest"
        snapshot_freeze = _bool_env("SNAPSHOT_FREEZE", True)
        
        # Get rate limiting configuration
        reqs_per_minute = _int_env("REQS_PER_MINUTE", 60)
        reqs_per_hour = _int_env("REQS_PER_HOUR", 3600)
        tokens_reserve = _int_env("TOKENS_RESERVE", 50)
        tokens_poll_sec = _int_env("TOKENS_POLL_SEC", 10)

        return cls(
            base_url=base_url,
            api_key=api_key,
            api_key_header=_env("ROCKETSOURCE_API_KEY_HEADER") or "Authorization",
            api_key_prefix=_env("ROCKETSOURCE_API_KEY_PREFIX") or "Bearer ",
            upload_path=_env("ROCKETSOURCE_UPLOAD_PATH") or "/scans",
            upload_file_field=_env("ROCKETSOURCE_UPLOAD_FILE_FIELD") or "file",
            scan_path=_env("ROCKETSOURCE_SCAN_PATH") or "/scans",
            scan_payload_template=_env("ROCKETSOURCE_SCAN_PAYLOAD")
            or '{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"Automated Scan"}}',
            status_path_template=_env("ROCKETSOURCE_STATUS_PATH_TEMPLATE") or "/scans/{scan_id}",
            results_path_template=_env("ROCKETSOURCE_RESULTS_PATH_TEMPLATE") or "/scans/{scan_id}/download?type=csv",
            poll_interval_s=_float_env("ROCKETSOURCE_POLL_INTERVAL", 3.0),
            poll_timeout_s=_float_env("ROCKETSOURCE_POLL_TIMEOUT", 600.0),
            log_level=_env("ROCKETSOURCE_LOG_LEVEL") or "INFO",
            
            # Retry configuration
            max_retries=max_retries,
            retry_delay=retry_delay,
            exponential_backoff=exponential_backoff,
            base_delay=base_delay,
            
            # Active scan handling
            wait_for_active_scans=wait_for_active_scans,
            max_wait_time=max_wait_time,
            
            # Retry status codes
            retry_status_codes=retry_status_codes,
            
            # Database configuration
            db_url=db_url,
            db_connect_timeout_s=db_connect_timeout_s,
            db_statement_timeout_ms=db_statement_timeout_ms,
            
            # Database table configuration - Using your env vars
            target_schema=target_schema,
            ungated_table=ungated_table,
            united_state_table=united_state_table,
            tirhak_schema=tirhak_schema,
            tirhak_table=tirhak_table,
            umair_schema=umair_schema,
            umair_table=umair_table,
            
            # Database performance settings
            db_batch_size=db_batch_size,
            db_enable_logging=db_enable_logging,
            
            # Sharding configuration
            shard_field=shard_field,
            shard_size=shard_size,
            paging_order=paging_order,
            snapshot_freeze=snapshot_freeze,
            
            # Rate limiting configuration
            reqs_per_minute=reqs_per_minute,
            reqs_per_hour=reqs_per_hour,
            tokens_reserve=tokens_reserve,
            tokens_poll_sec=tokens_poll_sec,
        )

    def get_database_config_dict(self) -> dict:
        """Get database configuration as a dictionary for DbService."""
        return {
            "db_url": self.db_url,
            "db_connect_timeout_s": self.db_connect_timeout_s,
            "db_statement_timeout_ms": self.db_statement_timeout_ms,
            "target_schema": self.target_schema,
            "ungated_table": self.ungated_table,
            "united_state_table": self.united_state_table,
            "tirhak_schema": self.tirhak_schema,
            "tirhak_table": self.tirhak_table,
            "umair_schema": self.umair_schema,
            "umair_table": self.umair_table,
            "db_batch_size": self.db_batch_size,
            "db_enable_logging": self.db_enable_logging,
        }