
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from typing import Iterable

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

import psycopg

from .errors import ConfigError


_LOG = logging.getLogger(__name__)


_ENSURE_TEST_UNITED_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS "Core Data"."test_united_state" (
    "ASIN" character varying PRIMARY KEY,
    "US_BB_Price" numeric,
    "Package_Weight" numeric,
    "FBA_Fee" numeric,
    "Referral_Fee" numeric,
    "Shipping_Cost" numeric,
    "Sales_Rank_Drops" integer,
    "Category" character varying,
    "created_at" timestamp without time zone,
    "last_updated" timestamp without time zone,
    "Seller" character varying
);
"""


_UPSERT_TEST_UNITED_STATE_SQL = """
INSERT INTO "Core Data"."test_united_state" (
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
    "Seller"
) VALUES (
    %(ASIN)s,
    %(US_BB_Price)s,
    %(Package_Weight)s,
    %(FBA_Fee)s,
    %(Referral_Fee)s,
    %(Shipping_Cost)s,
    %(Sales_Rank_Drops)s,
    %(Category)s,
    %(created_at)s,
    %(last_updated)s,
    %(Seller)s
)
ON CONFLICT ("ASIN") DO UPDATE
SET
    "US_BB_Price" = EXCLUDED."US_BB_Price",
    "Package_Weight" = EXCLUDED."Package_Weight",
    "FBA_Fee" = EXCLUDED."FBA_Fee",
    "Referral_Fee" = EXCLUDED."Referral_Fee",
    "Shipping_Cost" = EXCLUDED."Shipping_Cost",
    "Sales_Rank_Drops" = EXCLUDED."Sales_Rank_Drops",
    "Category" = EXCLUDED."Category",
    "created_at" = COALESCE("Core Data"."test_united_state"."created_at", EXCLUDED."created_at"),
    "last_updated" = EXCLUDED."last_updated",
    "Seller" = EXCLUDED."Seller";
"""


def _db_url() -> str:
    """Return the database URL from environment variables."""
    for key in ("ROCKETSOURCE_DB_URL", "DATABASE_URL", "DB_URL", "POSTGRES_URL"):
        v = os.environ.get(key)
        if v and v.strip():
            return v.strip()
    raise ConfigError("Missing database URL. Set ROCKETSOURCE_DB_URL (or DATABASE_URL/DB_URL/POSTGRES_URL).")


@dataclass(frozen=True)
class UngatedRow:
    """Row to insert into the ungated ASINs table."""

    asin: str
    status: str
    seller: str
    update_date: datetime


_SELECT_SQL = """
                WITH combined_data AS (
                    SELECT 
                        COALESCE(t.asin, u.asin) as asin,
                        'UNGATED' as status,
                        CASE 
                            WHEN t.status = 'UNGATED' AND u.status = 'UNGATED' THEN 'B'
                            WHEN t.status != 'UNGATED' AND u.status = 'UNGATED' THEN 'T'
                            WHEN t.status = 'UNGATED' AND u.status != 'UNGATED' THEN 'U'
                        END as seller
                    FROM "gated"."tirhak_gating_results_avg_tools_asins" t
                    FULL OUTER JOIN "Core Data"."umair_gating_results_tools" u 
                        ON t.asin = u.asin
                )
                SELECT asin, status, seller
                FROM combined_data
                WHERE seller IS NOT NULL
                ORDER BY seller, asin limit 10;
                """


_ENSURE_TEST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS "Core Data"."test_avg_book_sports_cd_tools_toys_ungated" (
    asin character varying PRIMARY KEY,
    status character varying NOT NULL,
    seller character varying,
    update_date timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


_UPSERT_TEST_TABLE_SQL = """
WITH combined_data AS (
    SELECT
        COALESCE(t.asin, u.asin) as asin,
        'UNGATED' as status,
        CASE
            WHEN t.status = 'UNGATED' AND u.status = 'UNGATED' THEN 'B'
            WHEN t.status != 'UNGATED' AND u.status = 'UNGATED' THEN 'T'
            WHEN t.status = 'UNGATED' AND u.status != 'UNGATED' THEN 'U'
        END as seller
    FROM "gated"."tirhak_gating_results_avg_tools_asins" t
    FULL OUTER JOIN "Core Data"."umair_gating_results_tools" u
        ON t.asin = u.asin
),
selected AS (
    SELECT asin, status, seller
    FROM combined_data
    WHERE seller IS NOT NULL
    ORDER BY seller, asin
    LIMIT 10
)
INSERT INTO "Core Data"."test_avg_book_sports_cd_tools_toys_ungated" (asin, status, seller, update_date)
SELECT asin, status, seller, CURRENT_TIMESTAMP
FROM selected
ON CONFLICT (asin) DO UPDATE
SET
    status = EXCLUDED.status,
    seller = EXCLUDED.seller,
    update_date = EXCLUDED.update_date
RETURNING asin, status, seller, update_date;
"""


class DbService:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or _db_url()
        self._connect_timeout_s: int | None = None
        self._statement_timeout_ms: int | None = None

        try:
            v = os.environ.get("ROCKETSOURCE_DB_CONNECT_TIMEOUT_S")
            if v and v.strip():
                self._connect_timeout_s = int(float(v))
        except Exception:
            self._connect_timeout_s = None

        try:
            v = os.environ.get("ROCKETSOURCE_DB_STATEMENT_TIMEOUT_MS")
            if v and v.strip():
                self._statement_timeout_ms = int(float(v))
        except Exception:
            self._statement_timeout_ms = None

    def fetch_new_ungated_rows(self) -> list[UngatedRow]:
        """Run the ungated ASINs query, store rows into the test table, and return them."""
        rows: list[UngatedRow] = []

        _LOG.info("DB: connecting...")
        t0 = time.time()

        with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
            with conn.cursor() as cur:
                if self._statement_timeout_ms is not None and self._statement_timeout_ms > 0:
                    cur.execute(f"SET LOCAL statement_timeout = {self._statement_timeout_ms}")

                _LOG.info('DB: ensuring schema "Core Data" exists...')
                cur.execute('CREATE SCHEMA IF NOT EXISTS "Core Data";')

                _LOG.info('DB: ensuring table "Core Data"."test_avg_book_sports_cd_tools_toys_ungated" exists...')
                cur.execute(_ENSURE_TEST_TABLE_SQL)

                _LOG.info("DB: selecting + storing ungated ASIN rows...")
                cur.execute(_UPSERT_TEST_TABLE_SQL)

                _LOG.info("DB: query executed (%.1fs). Fetching rows...", time.time() - t0)
                for r in cur.fetchall():
                    if not isinstance(r, tuple) or len(r) < 3:
                        continue
                    asin = r[0]
                    status = r[1]
                    seller = r[2]
                    update_date = r[3] if len(r) >= 4 else datetime.now()
                    rows.append(
                        UngatedRow(
                            asin=str(asin),
                            status=str(status),
                            seller=str(seller),
                            update_date=update_date,
                        )
                    )

        _LOG.info("DB: fetched %d rows (%.1fs)", len(rows), time.time() - t0)
        return rows

    def upsert_normalized_csv_to_test_united_state(self, csv_path: Path) -> int:
        def _parse_decimal(v: str | None) -> Decimal | None:
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            try:
                return Decimal(s)
            except Exception:
                return None

        def _parse_int(v: str | None) -> int | None:
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            try:
                return int(float(s))
            except Exception:
                return None

        def _parse_dt(v: str | None) -> datetime | None:
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            try:
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

        rows: list[dict[str, object]] = []
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            import csv as _csv

            r = _csv.DictReader(f)
            for row in r:
                asin = (row.get("ASIN") or "").strip()
                if not asin:
                    continue

                rows.append(
                    {
                        "ASIN": asin,
                        "US_BB_Price": _parse_decimal(row.get("US_BB_Price")),
                        "Package_Weight": _parse_decimal(row.get("Package_Weight")),
                        "FBA_Fee": _parse_decimal(row.get("FBA_Fee")),
                        "Referral_Fee": _parse_decimal(row.get("Referral_Fee")),
                        "Shipping_Cost": _parse_decimal(row.get("Shipping_Cost")),
                        "Sales_Rank_Drops": _parse_int(row.get("Sales_Rank_Drops")),
                        "Category": (row.get("Category") or "").strip() or None,
                        "created_at": _parse_dt(row.get("created_at")),
                        "last_updated": _parse_dt(row.get("last_updated")),
                        "Seller": (row.get("Seller") or "").strip() or None,
                    }
                )

        if not rows:
            return 0

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS "Core Data";')
                cur.execute(_ENSURE_TEST_UNITED_STATE_TABLE_SQL)
                cur.executemany(_UPSERT_TEST_UNITED_STATE_SQL, rows)

        _LOG.info('DB: upserted %d rows into "Core Data"."test_united_state"', len(rows))
        return len(rows)


def fetch_new_ungated_rows() -> list[UngatedRow]:
    """Run the ungated ASINs query, store rows into the test table, and return them."""
    return DbService().fetch_new_ungated_rows()


def fetch_and_insert_new_ungated_rows() -> list[UngatedRow]:
    """Backward-compatible alias for fetch_new_ungated_rows()."""
    return fetch_new_ungated_rows()


def asins_from_rows(rows: Iterable[UngatedRow]) -> list[str]:
    """Extract ASINs from UngatedRow objects."""
    return [r.asin for r in rows]


def upsert_normalized_csv_to_test_united_state(csv_path: Path) -> int:
    return DbService().upsert_normalized_csv_to_test_united_state(csv_path)
