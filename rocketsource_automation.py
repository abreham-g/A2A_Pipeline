import csv
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

from Script.config import RocketSourceConfig
from Script.db_service import asins_from_rows, fetch_new_ungated_rows, fetch_all_asins, upsert_normalized_csv_to_test_united_state
from Script.client import RocketSourceClient


class RocketSourceAutomation:
    def __init__(self, argv: list[str]) -> None:
        self._cfg = RocketSourceConfig.from_env()
        self._argv = argv

    @staticmethod
    def _write_asin_price_csv(path: Path, asins: list[str], price: float = 0.001) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ASIN", "PRICE"])
            for asin in asins:
                w.writerow([asin, price])

    @staticmethod
    def _normalize_results_csv(
        in_path: Path,
        out_path: Path,
        asin_to_seller: dict[str, str],
        now: datetime,
    ) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        created_at = now.strftime("%Y-%m-%d %H:%M:%S")

        def pick(row: dict[str, str], keys: list[str]) -> str:
            for k in keys:
                v = row.get(k)
                if isinstance(v, str) and v.strip() != "":
                    return v
            return ""

        with in_path.open("r", newline="", encoding="utf-8") as f_in:
            r = csv.DictReader(f_in)
            with out_path.open("w", newline="", encoding="utf-8") as f_out:
                w = csv.DictWriter(
                    f_out,
                    fieldnames=[
                        "ASIN",
                        "US_BB_Price",
                        "Package_Weight",
                        "FBA_Fee",
                        "Referral_Fee",
                        "Shipping_Cost",
                        "Category",
                        "created_at",
                        "last_updated",
                    ],
                )
                w.writeheader()
                for row in r:
                    asin = (row.get("ASIN") or "").strip()
                    w.writerow(
                        {
                            "ASIN": asin,
                            "US_BB_Price": pick(row, ["Buybox Price", "Buybox Price New", "Lowest Price New FBA"]),
                            "Package_Weight": pick(row, ["Weight"]),
                            "FBA_Fee": pick(row, ["FBA Fees"]),
                            "Referral_Fee": pick(row, ["Referral Fee"]),
                            "Shipping_Cost": pick(row, ["Inbound Shipping"]),
                            "Category": pick(row, ["Category"]),
                            "created_at": created_at,
                            "last_updated": created_at,
                        }
                    )

    def run(self) -> int:
        """Main run method - handles large ASIN lists by splitting into batches."""
        # Simplified approach: just get all ASINs from source tables
        asins = sorted(set(fetch_all_asins()))
        
        # Empty seller mapping since we're not doing status checks
        asin_to_seller = {}

        if not asins:
            print("No ASINs found in source tables.")
            return 0

        self._log = logging.getLogger("rocketsource")
        self._log.info("Found %d ASINs to scan", len(asins))

        # If there are too many ASINs, split them into batches
        # RocketSource might have limits on how many ASINs per scan
        max_asins_per_scan = 50000  # Adjust based on RocketSource limits
        asin_batches = self._split_asins_into_batches(asins, max_asins_per_scan)

        total_processed = 0
        for batch_num, batch_asins in enumerate(asin_batches, 1):
            self._log.info("Processing batch %d/%d with %d ASINs", 
                         batch_num, len(asin_batches), len(batch_asins))
            
            try:
                result = self._process_asin_batch(batch_asins, asin_to_seller)
                if result == 0:
                    total_processed += len(batch_asins)
                else:
                    self._log.error("Batch %d failed", batch_num)
                    return result
            except Exception as e:
                self._log.error("Error processing batch %d: %s", batch_num, e)
                return 1

        self._log.info("Successfully processed %d ASINs across %d batches", 
                      total_processed, len(asin_batches))
        return 0

    def _split_asins_into_batches(self, asins: List[str], batch_size: int) -> List[List[str]]:
        """Split ASINs into batches of manageable size."""
        batches = []
        for i in range(0, len(asins), batch_size):
            batches.append(asins[i:i + batch_size])
        return batches

    def _process_asin_batch(self, asins: List[str], asin_to_seller: dict[str, str]) -> int:
        """Process a single batch of ASINs."""
        with tempfile.TemporaryDirectory(prefix="rocketsource_") as tmp:
            tmp_dir = Path(tmp)
            input_csv = tmp_dir / "input.csv"
            out_path = tmp_dir / "results.csv"
            normalized_path = tmp_dir / "results_normalized.csv"

            self._write_asin_price_csv(input_csv, asins)

            # Create client and run scan
            client = RocketSourceClient(self._cfg)
            try:
                # Wait for any active scans to complete first
                self._log.info("Checking for active scans...")
                if client.wait_for_active_scans(timeout=1800):  # Wait up to 30 minutes
                    self._log.info("No active scans, proceeding with scan...")
                else:
                    self._log.error("Timed out waiting for active scans to complete")
                    return 1

                upload_id, scan_id, results_bytes = client.run_csv_scan(input_csv, out_path)
                
                # Write results to temp file
                out_path.write_bytes(results_bytes)
                
                if out_path.exists():
                    self._normalize_results_csv(out_path, normalized_path, asin_to_seller, datetime.now())
                    count = upsert_normalized_csv_to_test_united_state(normalized_path)
                    self._log.info("Upserted %d rows into target database table", count)
                    
                return 0
                
            except Exception as e:
                self._log.error("Scan failed: %s", e)
                return 1
            finally:
                client.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(RocketSourceAutomation(sys.argv[1:]).run())