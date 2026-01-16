import csv
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from Script.cli import main
from Script.config import RocketSourceConfig
from Script.db_service import asins_from_rows, fetch_new_ungated_rows, upsert_normalized_csv_to_test_united_state


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
    def _out_path(cfg: RocketSourceConfig, argv: list[str]) -> Path:
        out_arg: str | None = None
        for i, a in enumerate(argv):
            if a == "--out" and i + 1 < len(argv):
                out_arg = argv[i + 1]
                break

        if not out_arg:
            return cfg.data_dir / "scan_results.csv"

        p = Path(out_arg)
        if p.is_absolute():
            return p
        return cfg.data_dir / p

    @staticmethod
    def _strip_out_flag(argv: list[str]) -> list[str]:
        out: list[str] = []
        skip_next = False
        for a in argv:
            if skip_next:
                skip_next = False
                continue
            if a == "--out":
                skip_next = True
                continue
            if a.startswith("--out="):
                continue
            out.append(a)
        return out

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
                        "Seller",
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
                            "Seller": asin_to_seller.get(asin, ""),
                        }
                    )

    def _argv_without_positional_csv(self) -> list[str]:
        argv = list(self._argv)
        if argv and not argv[0].startswith("-"):
            return argv[1:]
        return argv

    def run(self) -> int:
        rows = fetch_new_ungated_rows()
        asins = sorted(set(asins_from_rows(rows)))
        asin_to_seller = {r.asin: r.seller for r in rows if r.asin}

        if not asins:
            print("No new ASINs to scan (query returned 0 rows).")
            return 0

        argv = self._argv_without_positional_csv()
        argv = self._strip_out_flag(argv)

        with tempfile.TemporaryDirectory(prefix="rocketsource_") as tmp:
            tmp_dir = Path(tmp)
            input_csv = tmp_dir / "input.csv"
            out_path = tmp_dir / "results.csv"
            normalized_path = tmp_dir / "results_normalized.csv"

            self._write_asin_price_csv(input_csv, asins)

            # Force the RocketSource CLI to write results into a temp file so we don't
            # persist any CSV output into Data/ (or user-provided --out paths).
            rc = main([str(input_csv), "--out", str(out_path), *argv])

            if rc == 0 and out_path.exists():
                self._normalize_results_csv(out_path, normalized_path, asin_to_seller, datetime.now())
                count = upsert_normalized_csv_to_test_united_state(normalized_path)
                logging.getLogger("rocketsource").info(
                    "Upserted %d rows into target database table (see DB logs for schema/table)",
                    count,
                )

            return rc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(RocketSourceAutomation(sys.argv[1:]).run())
