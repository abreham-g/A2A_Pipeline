import csv
import logging
import sys
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
                        "Sales_Rank_Drops",
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
                            "Sales_Rank_Drops": pick(
                                row,
                                [
                                    "Sales Rank Drops 30d",
                                    "Sales Rank Drops 60d",
                                    "Sales Rank Drops 90d",
                                    "Sales Rank Drops 180d",
                                ],
                            ),
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

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_csv = self._cfg.data_dir / f"db_input_{stamp}.csv"
        self._write_asin_price_csv(input_csv, asins)

        argv = self._argv_without_positional_csv()
        out_path = self._out_path(self._cfg, argv)
        rc = main([str(input_csv), *argv])

        if rc == 0 and out_path.exists():
            normalized_path = out_path.with_name(out_path.stem + "_normalized.csv")
            self._normalize_results_csv(out_path, normalized_path, asin_to_seller, datetime.now())
            logging.getLogger("rocketsource").info("Normalized results saved to %s", normalized_path)

            count = upsert_normalized_csv_to_test_united_state(normalized_path)
            logging.getLogger("rocketsource").info(
                'Upserted %d rows into "Core Data"."test_united_state"',
                count,
            )

        return rc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(RocketSourceAutomation(sys.argv[1:]).run())
