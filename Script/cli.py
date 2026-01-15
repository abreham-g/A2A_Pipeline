"""Command line entrypoint for running RocketSource scans."""

import argparse
import logging
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from .client import RocketSourceClient
from .config import RocketSourceConfig
from .errors import ConfigError, RocketSourceError


def setup_logging(level: str = "INFO") -> None:
    """Configure basic logging for CLI execution."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _resolve_out_path(cfg: RocketSourceConfig, out_arg: str | None) -> Path:
    """Resolve output path under Data/ unless an absolute path is provided."""
    if not out_arg:
        return cfg.data_dir / "scan_results.csv"

    p = Path(out_arg)
    if p.is_absolute():
        return p

    return cfg.data_dir / p


def _resolve_in_path(cfg: RocketSourceConfig, csv_arg: str) -> Path:
    """Resolve input path, defaulting to Data/ for bare filenames."""
    p = Path(csv_arg)
    if p.is_absolute():
        return p

    if p.parent == Path("."):
        return cfg.data_dir / p

    return p


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the rocketsource CLI."""
    p = argparse.ArgumentParser(prog="rocketsource")
    p.add_argument("csv", type=str, help="Path to CSV to upload")
    p.add_argument("--out", type=str, default=None, help="Output filename (saved under Data/) unless absolute")

    p.add_argument("--base-url", type=str, default=None)
    p.add_argument("--api-key-header", type=str, default=None)
    p.add_argument("--api-key-prefix", type=str, default=None)

    p.add_argument("--upload-path", type=str, default=None)
    p.add_argument("--upload-file-field", type=str, default=None)
    p.add_argument("--scan-path", type=str, default=None)
    p.add_argument("--scan-payload", type=str, default=None)
    p.add_argument("--status-path-template", type=str, default=None)
    p.add_argument("--results-path-template", type=str, default=None)

    p.add_argument("--interval", type=float, default=None)
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--log-level", type=str, default=None)
    return p


def _apply_overrides(cfg: RocketSourceConfig, args: argparse.Namespace) -> RocketSourceConfig:
    """Apply CLI overrides to the base configuration."""
    kw = {k: v for k, v in vars(args).items() if v is not None}
    kw.pop("csv", None)
    kw.pop("out", None)

    mapping = {
        "base_url": "base_url",
        "api_key_header": "api_key_header",
        "api_key_prefix": "api_key_prefix",
        "upload_path": "upload_path",
        "upload_file_field": "upload_file_field",
        "scan_path": "scan_path",
        "scan_payload": "scan_payload_template",
        "status_path_template": "status_path_template",
        "results_path_template": "results_path_template",
        "interval": "poll_interval_s",
        "timeout": "poll_timeout_s",
        "log_level": "log_level",
    }

    updates = {}
    for src, dst in mapping.items():
        if src in kw:
            updates[dst] = kw[src]

    if not updates:
        return cfg

    return RocketSourceConfig(**{**cfg.__dict__, **updates})


def main(argv: list[str]) -> int:
    """CLI entrypoint; returns process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        cfg = RocketSourceConfig.from_env()
        cfg = _apply_overrides(cfg, args)
    except ConfigError as e:
        print(str(e), file=sys.stderr)
        return 2

    setup_logging(cfg.log_level)
    log = logging.getLogger("rocketsource")

    csv_path = _resolve_in_path(cfg, args.csv)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 2

    out_path = _resolve_out_path(cfg, args.out)

    client = RocketSourceClient(cfg)
    try:
        upload_id, scan_id = client.run_csv_scan(csv_path, out_path)
        log.info("OK. upload_id=%s scan_id=%s out=%s", upload_id, scan_id, out_path)
        return 0
    except RocketSourceError as e:
        log.error("Failed: %s", e)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
