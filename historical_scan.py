import io
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

from Script.config import RocketSourceConfig
from Script.db_service import (
    fetch_new_ungated_rows,
    asins_from_rows,
    upsert_normalized_csv_to_test_united_state,
)
from Script.client import RocketSourceClient
from rocketsource_automation import RocketSourceAutomation


def _write_in_memory_csv(asins: List[str]) -> bytes:
    sio = io.StringIO()
    w = __import__("csv").writer(sio)
    w.writerow(["ASIN", "PRICE"])
    for a in asins:
        w.writerow([a, 0.001])
    return sio.getvalue().encode("utf-8")


def main(argv: List[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("historical_scan")

    cfg = RocketSourceConfig.from_env()

    # Fetch rows from DB and limit by env var
    rows = fetch_new_ungated_rows()
    asins_all = sorted(set(asins_from_rows(rows)))
    asin_to_seller = {r.asin: r.seller for r in rows if r.asin}

    if not asins_all:
        log.info("No ASINs found to scan.")
        return 0

    try:
        limit = int(os.environ.get("ROCKETSOURCE_ASIN_LIMIT", "1000"))
    except Exception:
        limit = 1000

    asins = asins_all[:limit]
    log.info("Preparing historical scan for %d ASINs (limit=%d)", len(asins), limit)

    client = RocketSourceClient(cfg)
    try:
        # Wait for active scans
        log.info("Checking for active scans...")
        if not client.wait_for_active_scans(timeout=cfg.max_wait_time if hasattr(cfg, 'max_wait_time') else 1800):
            log.error("Timed out waiting for active scans to complete")
            return 1

        csv_bytes = _write_in_memory_csv(asins)

        # Run scan entirely from in-memory bytes (handles API v3 'ok' responses)
        upload_id, scan_id, results_bytes = client.run_csv_scan_bytes("historical_input.csv", csv_bytes)
        log.info("Scan created. upload_id=%s scan_id=%s", upload_id, scan_id)

        # Normalize and upsert
        with tempfile.TemporaryDirectory(prefix="rocketsource_hist_") as tmp:
            tmp_dir = Path(tmp)
            out_path = tmp_dir / "results.csv"
            normalized_path = tmp_dir / "results_normalized.csv"

            out_path.write_bytes(results_bytes)

            # Also save raw results to repository Data/ folder with timestamped filename
            data_dir = Path(__file__).resolve().parent / "Data"
            try:
                data_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            raw_name = f"historical_results_{scan_id}_{ts}.csv"
            raw_path = data_dir / raw_name
            try:
                raw_path.write_bytes(results_bytes)
                log.info("Saved raw results to %s", raw_path)
            except Exception as e:
                log.warning("Failed to save raw results to Data/: %s", e)

            RocketSourceAutomation._normalize_results_csv(out_path, normalized_path, asin_to_seller, datetime.now())

            # Save normalized CSV to Data/ as well
            norm_name = f"historical_results_normalized_{scan_id}_{ts}.csv"
            norm_path = data_dir / norm_name
            try:
                # copy normalized_path contents to Data/
                norm_path.write_bytes(normalized_path.read_bytes())
                log.info("Saved normalized results to %s", norm_path)
            except Exception as e:
                log.warning("Failed to save normalized results to Data/: %s", e)

            count = upsert_normalized_csv_to_test_united_state(normalized_path)
            log.info("Upserted %d rows into target database table", count)

        return 0

    except Exception as e:
        log.error("Historical scan failed: %s", e)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))



