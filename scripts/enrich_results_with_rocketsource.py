"""Enrich a RocketSource results CSV with RocketSource averages.

Usage:
  python scripts/enrich_results_with_rocketsource.py <input.csv> [output.csv]

This reads the input CSV, finds an ASIN column, and for rows missing the UI fields
adds values from RocketSource (sales_estimate, price_avg_30d, price_avg_90d,
bsr_avg_30d, bsr_avg_90d). The script is standalone and does not modify other project
modules.
"""
from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path
from typing import Optional

# Ensure the project root (one level above this scripts/ folder) is on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from Script.config import RocketSourceConfig
from Script.rocketsource_api import RocketSourceAPI
from Script.db_service import fetch_new_ungated_rows, asins_from_rows


UI_FIELDS = [
    "Amazon Monthly Sold",
    "Average Price 30d",
    "Average Price 90d",
    "Average BSR 30d",
    "Average BSR 90d",
]


def find_asin_key(fieldnames: list[str]) -> Optional[str]:
    keys = {"ASIN", "asin", "Identifier", "identifier"}
    for fn in fieldnames:
        if fn and fn.strip() in keys:
            return fn
    return None


def enrich_csv(input_path: Path, output_path: Path, api: RocketSourceAPI) -> None:
    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        in_fieldnames = list(reader.fieldnames or [])

        asin_key = find_asin_key(in_fieldnames)
        if not asin_key:
            raise SystemExit("No ASIN/asin/Identifier column found in input CSV")

        out_fieldnames = in_fieldnames[:]
        for u in UI_FIELDS:
            if u not in out_fieldnames:
                out_fieldnames.append(u)

        rows = []
        for row in reader:
            # ensure UI fields exist; if empty, attempt to fetch from RocketSource
            asin = (row.get(asin_key) or "").strip()
            need_fetch = False
            for fld in UI_FIELDS:
                if not (row.get(fld) and str(row.get(fld)).strip()):
                    need_fetch = True
                    break

            if need_fetch and asin:
                try:
                    av = api.get_averages(asin)
                    if av:
                        if row.get("Amazon Monthly Sold") in (None, ""):
                            row["Amazon Monthly Sold"] = av.get("sales_estimate")
                        if row.get("Average Price 30d") in (None, ""):
                            row["Average Price 30d"] = av.get("price_avg_30d")
                        if row.get("Average Price 90d") in (None, ""):
                            row["Average Price 90d"] = av.get("price_avg_90d")
                        if row.get("Average BSR 30d") in (None, ""):
                            row["Average BSR 30d"] = av.get("bsr_avg_30d")
                        if row.get("Average BSR 90d") in (None, ""):
                            row["Average BSR 90d"] = av.get("bsr_avg_90d")
                except Exception as e:
                    logging.getLogger(__name__).debug("Failed to enrich %s: %s", asin, e)

            rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as outfh:
        writer = csv.DictWriter(outfh, fieldnames=out_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main(argv: list[str]) -> int:
    # Support two modes:
    # 1) CSV input: python scripts/enrich_results_with_rocketsource.py <input.csv> [output.csv]
    # 2) DB mode:   python scripts/enrich_results_with_rocketsource.py --from-db [output.csv]
    if len(argv) < 2:
        print("Usage: python scripts/enrich_results_with_rocketsource.py <input.csv> [output.csv]\n       or: python scripts/enrich_results_with_rocketsource.py --from-db [output.csv]")
        return 2

    mode = argv[1]

    # Load config and create API helper
    try:
        cfg = RocketSourceConfig.from_env()
    except Exception as e:
        print("Missing RocketSource config in environment:", e)
        return 1

    api = RocketSourceAPI(cfg)

    if mode == "--from-db":
        out_arg = argv[2] if len(argv) > 2 else None
        output_path = Path(out_arg) if out_arg else Path("Data") / "historical_results_enriched_from_db.csv"

        # Fetch ASINs from DB
        try:
            rows = fetch_new_ungated_rows()
            asins = asins_from_rows(rows)
        except Exception as e:
            print("Failed to fetch ASINs from DB:", e)
            return 1

        # Build a minimal rows list with ASINs and enrich
        from_rows = []
        for a in asins:
            r = {"ASIN": a}
            from_rows.append(r)

        # Write enriched output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as outfh:
            fieldnames = ["ASIN"] + UI_FIELDS
            writer = csv.DictWriter(outfh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for r in from_rows:
                asin = r.get("ASIN")
                try:
                    av = api.get_averages(asin)
                    row = {"ASIN": asin}
                    row["Amazon Monthly Sold"] = av.get("sales_estimate")
                    row["Average Price 30d"] = av.get("price_avg_30d")
                    row["Average Price 90d"] = av.get("price_avg_90d")
                    row["Average BSR 30d"] = av.get("bsr_avg_30d")
                    row["Average BSR 90d"] = av.get("bsr_avg_90d")
                except Exception as e:
                    logging.getLogger(__name__).debug("Failed to enrich %s: %s", asin, e)
                    row = {"ASIN": asin}
                writer.writerow(row)

        print("Wrote enriched CSV from DB:", output_path)
        return 0

    # CSV input mode
    input_path = Path(mode)
    if not input_path.exists():
        print("Input file not found:", input_path)
        return 2

    output_path = Path(argv[2]) if len(argv) > 2 else input_path.with_name(input_path.stem + "_enriched.csv")
    enrich_csv(input_path, output_path, api)
    print("Wrote enriched CSV:", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
