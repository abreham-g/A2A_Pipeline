"""Configuration loading for RocketSource automation."""

import os
from dataclasses import dataclass
from pathlib import Path

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
    scan_payload_template: str = '{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"Automated Scan"}}'

    status_path_template: str = "/scans/{scan_id}"
    results_path_template: str = "/scans/{scan_id}/download?type=csv"

    poll_interval_s: float = 3.0
    poll_timeout_s: float = 600.0

    log_level: str = "INFO"

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
        )
