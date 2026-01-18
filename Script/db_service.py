
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import Iterable

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

import psycopg
from psycopg import sql

from .errors import ConfigError


_LOG = logging.getLogger(__name__)


def _redact_dsn(dsn: str) -> str:
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    if dsn.startswith("postgres://") or dsn.startswith("postgresql://"):
        try:
            u = urlparse(dsn)
            netloc = u.hostname or ""
            if u.port:
                netloc = f"{netloc}:{u.port}"
            if u.username:
                netloc = f"{u.username}@{netloc}" if netloc else f"{u.username}@"
            path = u.path or ""
            return f"{u.scheme}://{netloc}{path}"
        except Exception:
            return "postgresql://<redacted>"
    parts = []
    for token in dsn.split():
        if token.lower().startswith("password="):
            parts.append("password=<redacted>")
        else:
            parts.append(token)
    return " ".join(parts)


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _qual(schema_name: str, table_name: str):
    return sql.SQL(".").join([sql.Identifier(schema_name), sql.Identifier(table_name)])


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


class DbService:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or _db_url()
        self._connect_timeout_s: int | None = None
        self._statement_timeout_ms: int | None = None

        self._target_schema = _env_str("ROCKETSOURCE_TARGET_SCHEMA")
        self._ungated_table = _env_str(
            "ROCKETSOURCE_UNGATED_TABLE",
            "test_avg_book_sports_cd_tools_toys_ungated",
        )
        self._united_state_table = _env_str("ROCKETSOURCE_UNITED_STATE_TABLE")

        self._tirhak_schema = _env_str("ROCKETSOURCE_TIRHAK_SCHEMA")
        self._tirhak_table = _env_str("ROCKETSOURCE_TIRHAK_TABLE")
        self._umair_schema = _env_str("ROCKETSOURCE_UMAIR_SCHEMA")
        self._umair_table = _env_str("ROCKETSOURCE_UMAIR_TABLE")

        # self._asin_limit = _env_int("ROCKETSOURCE_ASIN_LIMIT")

        _LOG.info("DB: target=%s", _redact_dsn(self._dsn))
        _LOG.info(
            'DB: output tables=%s.%s (ungated), %s.%s (united_state)',
            self._target_schema,
            self._ungated_table,
            self._target_schema,
            self._united_state_table,
        )
        _LOG.info(
            'DB: source tables=%s.%s (tirhak), %s.%s (umair)',
            self._tirhak_schema,
            self._tirhak_table,
            self._umair_schema,
            self._umair_table,
        )
        # _LOG.info("DB: asin_limit=%s", self._asin_limit)

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

    def _ensure_schema(self, cur, schema_name: str) -> None:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))

    def _ensure_ungated_table(self, cur) -> None:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    asin character varying PRIMARY KEY,
                    status character varying NOT NULL,
                    seller character varying,
                    update_date timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            ).format(_qual(self._target_schema, self._ungated_table))
        )

    # def _ensure_united_state_table(self, cur) -> None:
    #     cur.execute(
    #         sql.SQL(
    #             """
    #             CREATE TABLE IF NOT EXISTS {} (
    #                 "ASIN" character varying PRIMARY KEY,
    #                 "US_BB_Price" numeric,
    #                 "Package_Weight" numeric,
    #                 "FBA_Fee" numeric,
    #                 "Referral_Fee" numeric,
    #                 "Shipping_Cost" numeric,
    #                 "Category" character varying,
    #                 "created_at" timestamp without time zone,
    #                 "last_updated" timestamp without time zone,
    #                 "Seller" character varying
    #             );
    #             """
    #         ).format(_qual(self._target_schema, self._united_state_table))
    #     )
    def _ensure_united_state_table(self, cur) -> None:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    "ASIN" character varying PRIMARY KEY,
                    "US_BB_Price" numeric,
                    "Package_Weight" numeric,
                    "FBA_Fee" numeric,
                    "Referral_Fee" numeric,
                    "Shipping_Cost" numeric,
                    "Sales_Rank_Drops" integer DEFAULT 0,
                    "Category" character varying,
                    "created_at" timestamp without time zone,
                    "last_updated" timestamp without time zone,
                    "Seller" character varying
                );
                """
            ).format(_qual(self._target_schema, self._united_state_table))
        )

    def _upsert_ungated_rows_sql(self) -> sql.SQL:
        return sql.SQL(
            """
            WITH combined_data AS (
                SELECT
                    COALESCE(t.asin, u.asin) as asin,
                    'UNGATED' as status,
                    CASE
                        WHEN t.status = 'UNGATED' AND u.status = 'UNGATED' THEN 'B'
                        WHEN t.status != 'UNGATED' AND u.status = 'UNGATED' THEN 'T'
                        WHEN t.status = 'UNGATED' AND u.status != 'UNGATED' THEN 'U'
                    END as seller
                FROM {} t
                FULL OUTER JOIN {} u
                    ON t.asin = u.asin
            ),
            selected AS (
                SELECT asin, status, seller
                FROM combined_data
                WHERE seller IS NOT NULL
                ORDER BY seller, asin
            )
            INSERT INTO {} (asin, status, seller, update_date)
            SELECT asin, status, seller, CURRENT_TIMESTAMP
            FROM selected
            ON CONFLICT (asin) DO UPDATE
            SET
                status = EXCLUDED.status,
                seller = EXCLUDED.seller,
                update_date = EXCLUDED.update_date
            RETURNING asin, status, seller, update_date;
            """
        ).format(
            _qual(self._tirhak_schema, self._tirhak_table),
            _qual(self._umair_schema, self._umair_table),
            _qual(self._target_schema, self._ungated_table),
            limit=sql.Literal(self._asin_limit),
        )

    # def _upsert_united_state_sql(self) -> sql.SQL:
    #     dest = _qual(self._target_schema, self._united_state_table)
    #     return sql.SQL(
    #         """
    #         INSERT INTO {} (
    #             "ASIN",
    #             "US_BB_Price",
    #             "Package_Weight",
    #             "FBA_Fee",
    #             "Referral_Fee",
    #             "Shipping_Cost",
    #             "Category",
    #             "created_at",
    #             "last_updated",
    #             "Seller"
    #         ) VALUES (
    #             %(ASIN)s,
    #             %(US_BB_Price)s,
    #             %(Package_Weight)s,
    #             %(FBA_Fee)s,
    #             %(Referral_Fee)s,
    #             %(Shipping_Cost)s,
    #             %(Category)s,
    #             %(created_at)s,
    #             %(last_updated)s,
    #             %(Seller)s
    #         )
    #         ON CONFLICT ("ASIN") DO UPDATE
    #         SET
    #             "US_BB_Price" = EXCLUDED."US_BB_Price",
    #             "Package_Weight" = EXCLUDED."Package_Weight",
    #             "FBA_Fee" = EXCLUDED."FBA_Fee",
    #             "Referral_Fee" = EXCLUDED."Referral_Fee",
    #             "Shipping_Cost" = EXCLUDED."Shipping_Cost",
    #             "Category" = EXCLUDED."Category",
    #             "created_at" = COALESCE({}."created_at", EXCLUDED."created_at"),
    #             "last_updated" = EXCLUDED."last_updated",
    #             "Seller" = EXCLUDED."Seller";
    #         """
    #     ).format(dest, dest)
    def _upsert_united_state_sql(self) -> sql.SQL:
        dest = _qual(self._target_schema, self._united_state_table)
        return sql.SQL(
            """
            INSERT INTO {} (
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
                "created_at" = COALESCE({}."created_at", EXCLUDED."created_at"),
                "last_updated" = EXCLUDED."last_updated",
                "Seller" = EXCLUDED."Seller";
            """
        ).format(dest, dest)

    def fetch_new_ungated_rows(self) -> list[UngatedRow]:
        """Run the ungated ASINs query, store rows into the test table, and return them."""
        rows: list[UngatedRow] = []

        _LOG.info("DB: connecting...")
        t0 = time.time()

        with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
            with conn.cursor() as cur:
                if self._statement_timeout_ms is not None and self._statement_timeout_ms > 0:
                    cur.execute(f"SET LOCAL statement_timeout = {self._statement_timeout_ms}")

                _LOG.info('DB: ensuring schema "%s" exists...', self._target_schema)
                self._ensure_schema(cur, self._target_schema)

                _LOG.info('DB: ensuring table "%s"."%s" exists...', self._target_schema, self._ungated_table)
                self._ensure_ungated_table(cur)

                _LOG.info("DB: selecting + storing ungated ASIN rows...")
                cur.execute(self._upsert_ungated_rows_sql())

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

    # def upsert_normalized_csv_to_test_united_state(self, csv_path: Path) -> int:
    #     def _parse_decimal(v: str | None) -> Decimal | None:
    #         if v is None:
    #             return None
    #         s = v.strip()
    #         if s == "":
    #             return None
    #         try:
    #             return Decimal(s)
    #         except Exception:
    #             return None

    #     def _parse_dt(v: str | None) -> datetime | None:
    #         if v is None:
    #             return None
    #         s = v.strip()
    #         if s == "":
    #             return None
    #         try:
    #             return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    #         except Exception:
    #             return None

    #     rows: list[dict[str, object]] = []
    #     with csv_path.open("r", newline="", encoding="utf-8") as f:
    #         import csv as _csv

    #         r = _csv.DictReader(f)
    #         for row in r:
    #             asin = (row.get("ASIN") or "").strip()
    #             if not asin:
    #                 continue

    #             rows.append(
    #                 {
    #                     "ASIN": asin,
    #                     "US_BB_Price": _parse_decimal(row.get("US_BB_Price")),
    #                     "Package_Weight": _parse_decimal(row.get("Package_Weight")),
    #                     "FBA_Fee": _parse_decimal(row.get("FBA_Fee")),
    #                     "Referral_Fee": _parse_decimal(row.get("Referral_Fee")),
    #                     "Shipping_Cost": _parse_decimal(row.get("Shipping_Cost")),
    #                     "Category": (row.get("Category") or "").strip() or None,
    #                     "created_at": _parse_dt(row.get("created_at")),
    #                     "last_updated": _parse_dt(row.get("last_updated")),
    #                     "Seller": (row.get("Seller") or "").strip() or None,
    #                 }
    #             )

    #     if not rows:
    #         return 0

    #     with psycopg.connect(self._dsn) as conn:
    #         with conn.cursor() as cur:
    #             self._ensure_schema(cur, self._target_schema)
    #             self._ensure_united_state_table(cur)
    #             cur.executemany(self._upsert_united_state_sql(), rows)

    #         conn.commit()

    #     try:
    #         with psycopg.connect(self._dsn) as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute(
    #                     sql.SQL("SELECT COUNT(*) FROM {};").format(
    #                         _qual(self._target_schema, self._united_state_table)
    #                     )
    #                 )
    #                 total = cur.fetchone()[0]
    #         _LOG.info('DB: "%s"."%s" total rows=%s', self._target_schema, self._united_state_table, total)
    #     except Exception:
    #         pass

    #     _LOG.info('DB: upserted %d rows into "%s"."%s"', len(rows), self._target_schema, self._united_state_table)
    #     return len(rows)
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
                return 0  # Default to 0 if empty
            try:
                return int(float(s))
            except Exception:
                return 0  # Default to 0 if invalid

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
                        "Sales_Rank_Drops": _parse_int(row.get("Sales_Rank_Drops")),  # Added this line
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
                self._ensure_schema(cur, self._target_schema)
                self._ensure_united_state_table(cur)
                cur.executemany(self._upsert_united_state_sql(), rows)

            conn.commit()

        try:
            with psycopg.connect(self._dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT COUNT(*) FROM {};").format(
                            _qual(self._target_schema, self._united_state_table)
                        )
                    )
                    total = cur.fetchone()[0]
            _LOG.info('DB: "%s"."%s" total rows=%s', self._target_schema, self._united_state_table, total)
        except Exception:
            pass

        _LOG.info('DB: upserted %d rows into "%s"."%s"', len(rows), self._target_schema, self._united_state_table)
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


