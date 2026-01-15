"""Utility helpers for formatting and writing results."""

import csv
from pathlib import Path
from typing import Any

from .errors import ApiResponseError


def _extract_rows(data: Any) -> list[dict[str, Any]]:
    """Normalize a JSON payload into a list of flat dict rows."""
    if isinstance(data, list):
        if all(isinstance(x, dict) for x in data):
            return data
        raise ApiResponseError("JSON results list is not a list of objects")

    if isinstance(data, dict):
        for key in ("data", "results", "items"):
            v = data.get(key)
            if isinstance(v, list) and all(isinstance(x, dict) for x in v):
                return v
        return [data]

    raise ApiResponseError("Unsupported JSON results structure")


def write_json_as_csv(data: Any, out_path: Path) -> None:
    """Write a JSON results payload to a CSV file."""
    rows = _extract_rows(data)
    fieldnames: list[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames})
