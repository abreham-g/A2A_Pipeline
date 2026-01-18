import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import Iterable, Optional, List, Dict, Any
import csv as _csv

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
    """Redact password from database connection string for logging."""
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


def _env_str(name: str, default: str = "") -> str:
    """Get environment variable as string with default."""
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_int(name: str, default: int = 0) -> int:
    """Get environment variable as integer with default."""
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip().lower()
    return v in ("true", "yes", "1", "on")


def _qual(schema_name: str, table_name: str) -> sql.Composed:
    """Create a qualified table identifier."""
    return sql.SQL(".").join([sql.Identifier(schema_name), sql.Identifier(table_name)])


def _db_url() -> str:
    """Return the database URL from environment variables."""
    env_vars = ("ROCKETSOURCE_DB_URL", "DATABASE_URL", "DB_URL", "POSTGRES_URL")
    for key in env_vars:
        v = os.environ.get(key)
        if v and v.strip():
            return v.strip()
    raise ConfigError(
        f"Missing database URL. Set one of: {', '.join(env_vars)}."
    )


@dataclass(frozen=True)
class UngatedRow:
    """Row to insert into the ungated ASINs table."""

    asin: str
    status: str
    seller: str
    update_date: datetime


class DbService:
    """Database service for managing RocketSource data."""
    
    def __init__(self, dsn: Optional[str] = None) -> None:
        """Initialize database service with connection string and configuration."""
        self._dsn = dsn or _db_url()
        self._connect_timeout_s: Optional[int] = None
        self._statement_timeout_ms: Optional[int] = None

        # Target tables configuration
        self._target_schema = _env_str("ROCKETSOURCE_TARGET_SCHEMA", "public")
        self._ungated_table = _env_str(
            "ROCKETSOURCE_UNGATED_TABLE",
            "test_avg_book_sports_cd_tools_toys_ungated",
        )
        self._united_state_table = _env_str("ROCKETSOURCE_UNITED_STATE_TABLE", "test_united_state")

        # Source tables configuration
        self._tirhak_schema = _env_str("ROCKETSOURCE_TIRHAK_SCHEMA", "public")
        self._tirhak_table = _env_str("ROCKETSOURCE_TIRHAK_TABLE", "tirhak_gating_results")
        self._umair_schema = _env_str("ROCKETSOURCE_UMAIR_SCHEMA", "public")
        self._umair_table = _env_str("ROCKETSOURCE_UMAIR_TABLE", "umair_gating_results")

        # Performance and logging settings
        self._batch_size = _env_int("ROCKETSOURCE_DB_BATCH_SIZE", 1000)
        self._enable_logging = _env_bool("ROCKETSOURCE_DB_ENABLE_LOGGING", True)

        # Log configuration
        if self._enable_logging:
            _LOG.info("DB: target=%s", _redact_dsn(self._dsn))
            _LOG.info(
                'DB: output tables=%s.%s (ungated), %s.%s (united_state)',
                self._target_schema, self._ungated_table,
                self._target_schema, self._united_state_table
            )
            _LOG.info(
                'DB: source tables=%s.%s (tirhak), %s.%s (umair)',
                self._tirhak_schema, self._tirhak_table,
                self._umair_schema, self._umair_table
            )
            _LOG.info("DB: batch_size=%d", self._batch_size)

        # Timeout configuration
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
        """Create schema if it doesn't exist."""
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))

    def _ensure_ungated_table(self, cur) -> None:
        """Create ungated table if it doesn't exist."""
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

    def _ensure_united_state_table(self, cur) -> None:
        """Create united_state table if it doesn't exist."""
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    "ASIN" character varying PRIMARY KEY,
                    "US_BB_Price" numeric NOT NULL DEFAULT 0,
                    "Package_Weight" numeric,
                    "FBA_Fee" numeric NOT NULL DEFAULT 0,
                    "Referral_Fee" numeric NOT NULL DEFAULT 0,
                    "Shipping_Cost" numeric NOT NULL DEFAULT 0,
                    "Sales_Rank_Drops" integer DEFAULT 0,
                    "Category" character varying,
                    "created_at" timestamp without time zone,
                    "last_updated" timestamp without time zone,
                    "Seller" character varying
                );
                """
            ).format(_qual(self._target_schema, self._united_state_table))
        )

    def _upsert_ungated_rows_sql(self) -> sql.Composed:
        """Generate SQL for upserting ungated rows."""
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
        )

    def _upsert_united_state_sql(self) -> sql.Composed:
        """Generate SQL for upserting united_state rows."""
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
                "last_updated" = EXCLUDED."last_updated"
                -- Note: Seller column is NOT updated - existing Seller value is preserved
            ;
            """
        ).format(dest, dest)

    def fetch_new_ungated_rows(self) -> List[UngatedRow]:
        """Run the ungated ASINs query, store rows into the test table, and return them."""
        rows: List[UngatedRow] = []

        if self._enable_logging:
            _LOG.info("DB: connecting...")
        
        t0 = time.time()

        try:
            with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
                with conn.cursor() as cur:
                    if self._statement_timeout_ms is not None and self._statement_timeout_ms > 0:
                        cur.execute(f"SET LOCAL statement_timeout = {self._statement_timeout_ms}")

                    if self._enable_logging:
                        _LOG.info('DB: ensuring schema "%s" exists...', self._target_schema)
                    self._ensure_schema(cur, self._target_schema)

                    if self._enable_logging:
                        _LOG.info('DB: ensuring table "%s"."%s" exists...', self._target_schema, self._ungated_table)
                    self._ensure_ungated_table(cur)

                    if self._enable_logging:
                        _LOG.info("DB: selecting + storing ungated ASIN rows...")
                    cur.execute(self._upsert_ungated_rows_sql())

                    if self._enable_logging:
                        _LOG.info("DB: query executed (%.1fs). Fetching rows...", time.time() - t0)
                    
                    for r in cur.fetchall():
                        if not isinstance(r, tuple) or len(r) < 3:
                            continue
                        asin = r[0]
                        status = r[1]
                        seller = r[2]
                        update_date = r[3] if len(r) >= 4 else datetime.now()
                        
                        # Validate data
                        if not asin or not status:
                            continue
                            
                        rows.append(
                            UngatedRow(
                                asin=str(asin).strip(),
                                status=str(status).strip(),
                                seller=str(seller).strip() if seller else "",
                                update_date=update_date,
                            )
                        )

                    conn.commit()
                    
        except Exception as e:
            _LOG.error("DB: Error fetching ungated rows: %s", e)
            raise

        if self._enable_logging:
            _LOG.info("DB: fetched %d rows (%.1fs)", len(rows), time.time() - t0)
        
        return rows

    # def upsert_normalized_csv_to_test_united_state(self, csv_path: Path) -> int:
        """Upsert normalized CSV data into the united_state table."""
        
        def _parse_decimal_required(v: Optional[str]) -> Decimal:
            """Parse string to Decimal safely, returning 0.00 for null/empty."""
            if v is None:
                return Decimal('0.00')
            s = v.strip()
            if s == "":
                return Decimal('0.00')
            try:
                # Remove any non-numeric characters except decimal point and minus sign
                cleaned = ''.join(c for c in s if c.isdigit() or c in '.-')
                if not cleaned:
                    return Decimal('0.00')
                return Decimal(cleaned)
            except (InvalidOperation, ValueError):
                return Decimal('0.00')

        def _parse_decimal_optional(v: Optional[str]) -> Optional[Decimal]:
            """Parse string to Decimal safely, returning None for null/empty."""
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            try:
                cleaned = ''.join(c for c in s if c.isdigit() or c in '.-')
                if not cleaned:
                    return None
                return Decimal(cleaned)
            except (InvalidOperation, ValueError):
                return None

        def _parse_int(v: Optional[str]) -> int:
            """Parse string to integer safely."""
            if v is None:
                return 0
            s = v.strip()
            if s == "":
                return 0
            try:
                # Try to parse as float first to handle decimal strings
                return int(float(s))
            except (ValueError, TypeError):
                return 0

        def _parse_dt(v: Optional[str]) -> Optional[datetime]:
            """Parse string to datetime safely."""
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            
            # Try multiple date formats
            date_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y"
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            
            return None

        if self._enable_logging:
            _LOG.info("DB: processing CSV file: %s", csv_path)
        
        rows: List[Dict[str, Any]] = []
        processed_count = 0
        skipped_count = 0
        
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                reader = _csv.DictReader(f)
                
                # Validate required columns
                required_columns = {"ASIN"}
                missing_columns = required_columns - set(reader.fieldnames or [])
                if missing_columns:
                    raise ValueError(f"Missing required columns in CSV: {missing_columns}")
                
                for row_num, row in enumerate(reader, start=1):
                    asin = (row.get("ASIN") or "").strip()
                    if not asin:
                        skipped_count += 1
                        if self._enable_logging and skipped_count <= 10:
                            _LOG.warning("DB: Skipping row %d: missing ASIN", row_num)
                        continue

                    processed_data = {
                        "ASIN": asin,
                        "US_BB_Price": _parse_decimal_required(row.get("US_BB_Price")),
                        "Package_Weight": _parse_decimal_optional(row.get("Package_Weight")),
                        "FBA_Fee": _parse_decimal_required(row.get("FBA_Fee")),
                        "Referral_Fee": _parse_decimal_required(row.get("Referral_Fee")),
                        "Shipping_Cost": _parse_decimal_required(row.get("Shipping_Cost")),
                        "Sales_Rank_Drops": _parse_int(row.get("Sales_Rank_Drops")),
                        "Category": (row.get("Category") or "").strip() or None,
                        "created_at": _parse_dt(row.get("created_at")),
                        "last_updated": _parse_dt(row.get("last_updated")),
                        "Seller": (row.get("Seller") or "").strip() or None,
                    }
                    
                    rows.append(processed_data)
                    processed_count += 1
                    
                    # Batch insert if we have enough rows
                    if len(rows) >= self._batch_size:
                        self._batch_insert_united_state(rows)
                        rows = []
        
        except Exception as e:
            _LOG.error("DB: Error reading CSV file %s: %s", csv_path, e)
            raise

        if self._enable_logging:
            _LOG.info("DB: processed %d rows, skipped %d rows", processed_count, skipped_count)

        if not rows:
            return 0

        # Insert remaining rows
        inserted_count = self._batch_insert_united_state(rows)

        # Get total count
        try:
            total = self._get_table_count(self._target_schema, self._united_state_table)
            if self._enable_logging:
                _LOG.info('DB: "%s"."%s" total rows=%s', self._target_schema, self._united_state_table, total)
        except Exception as e:
            if self._enable_logging:
                _LOG.warning("DB: Could not get table count: %s", e)

        return inserted_count

    def upsert_normalized_csv_to_test_united_state(self, csv_path: Path) -> int:
        """Upsert normalized CSV data into the united_state table."""
        
        def _parse_decimal_required(v: Optional[str]) -> Decimal:
            """Parse string to Decimal safely, returning 0.00 for null/empty."""
            if v is None:
                return Decimal('0.00')
            s = v.strip()
            if s == "":
                return Decimal('0.00')
            try:
                # Remove any non-numeric characters except decimal point and minus sign
                cleaned = ''.join(c for c in s if c.isdigit() or c in '.-')
                if not cleaned:
                    return Decimal('0.00')
                return Decimal(cleaned)
            except (InvalidOperation, ValueError):
                return Decimal('0.00')

        def _parse_decimal_optional(v: Optional[str]) -> Optional[Decimal]:
            """Parse string to Decimal safely, returning None for null/empty."""
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            try:
                cleaned = ''.join(c for c in s if c.isdigit() or c in '.-')
                if not cleaned:
                    return None
                return Decimal(cleaned)
            except (InvalidOperation, ValueError):
                return None

        def _parse_int(v: Optional[str]) -> int:
            """Parse string to integer safely."""
            if v is None:
                return 0
            s = v.strip()
            if s == "":
                return 0
            try:
                # Try to parse as float first to handle decimal strings
                return int(float(s))
            except (ValueError, TypeError):
                return 0

        def _parse_dt(v: Optional[str]) -> Optional[datetime]:
            """Parse string to datetime safely."""
            if v is None:
                return None
            s = v.strip()
            if s == "":
                return None
            
            # Try multiple date formats
            date_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y"
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            
            return None

        if self._enable_logging:
            _LOG.info("DB: processing CSV file: %s", csv_path)
        
        rows: List[Dict[str, Any]] = []
        processed_count = 0
        skipped_count = 0
        
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                reader = _csv.DictReader(f)
                
                # Validate required columns
                required_columns = {"ASIN"}
                missing_columns = required_columns - set(reader.fieldnames or [])
                if missing_columns:
                    raise ValueError(f"Missing required columns in CSV: {missing_columns}")
                
                for row_num, row in enumerate(reader, start=1):
                    asin = (row.get("ASIN") or "").strip()
                    if not asin:
                        skipped_count += 1
                        if self._enable_logging and skipped_count <= 10:
                            _LOG.warning("DB: Skipping row %d: missing ASIN", row_num)
                        continue

                    processed_data = {
                        "ASIN": asin,
                        "US_BB_Price": _parse_decimal_required(row.get("US_BB_Price")),
                        "Package_Weight": _parse_decimal_optional(row.get("Package_Weight")),
                        "FBA_Fee": _parse_decimal_required(row.get("FBA_Fee")),  # REQUIRED
                        "Referral_Fee": _parse_decimal_required(row.get("Referral_Fee")),
                        "Shipping_Cost": _parse_decimal_required(row.get("Shipping_Cost")),
                        "Sales_Rank_Drops": _parse_int(row.get("Sales_Rank_Drops")),
                        "Category": (row.get("Category") or "").strip() or None,
                        "created_at": _parse_dt(row.get("created_at")),
                        "last_updated": _parse_dt(row.get("last_updated")),
                        "Seller": (row.get("Seller") or "").strip() or None,
                    }
                    
                    rows.append(processed_data)
                    processed_count += 1
                    
                    # Batch insert if we have enough rows
                    if len(rows) >= self._batch_size:
                        self._batch_insert_united_state(rows)
                        rows = []
        
        except Exception as e:
            _LOG.error("DB: Error reading CSV file %s: %s", csv_path, e)
            raise

        if self._enable_logging:
            _LOG.info("DB: processed %d rows, skipped %d rows", processed_count, skipped_count)

        if not rows:
            return 0

        # Insert remaining rows
        inserted_count = self._batch_insert_united_state(rows)

        # Get total count
        try:
            total = self._get_table_count(self._target_schema, self._united_state_table)
            if self._enable_logging:
                _LOG.info('DB: "%s"."%s" total rows=%s', self._target_schema, self._united_state_table, total)
        except Exception as e:
            if self._enable_logging:
                _LOG.warning("DB: Could not get table count: %s", e)

        return inserted_count

    def _batch_insert_united_state(self, rows: List[Dict[str, Any]]) -> int:
        """Batch insert rows into united_state table."""
        if not rows:
            return 0
        
        t0 = time.time()
        inserted_count = 0
        
        try:
            with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
                with conn.cursor() as cur:
                    self._ensure_schema(cur, self._target_schema)
                    self._ensure_united_state_table(cur)
                    
                    # Use executemany for batch insertion
                    cur.executemany(self._upsert_united_state_sql(), rows)
                    inserted_count = len(rows)
                    
                conn.commit()
                
        except Exception as e:
            _LOG.error("DB: Error batch inserting %d rows: %s", len(rows), e)
            # Try inserting one by one to identify problematic rows
            inserted_count = self._insert_one_by_one(rows)
        
        if self._enable_logging:
            _LOG.info('DB: upserted %d rows into "%s"."%s" in %.1fs', 
                     inserted_count, self._target_schema, self._united_state_table, time.time() - t0)
        
        return inserted_count

    def _insert_one_by_one(self, rows: List[Dict[str, Any]]) -> int:
        """Insert rows one by one to handle errors individually."""
        inserted_count = 0
        
        with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
            with conn.cursor() as cur:
                self._ensure_schema(cur, self._target_schema)
                self._ensure_united_state_table(cur)
                
                for row in rows:
                    try:
                        cur.execute(self._upsert_united_state_sql(), row)
                        inserted_count += 1
                    except Exception as e:
                        _LOG.warning("DB: Failed to insert row with ASIN=%s: %s", row.get("ASIN"), e)
                        conn.rollback()  # Rollback failed transaction
                        # Continue with next row
                        continue
                    else:
                        conn.commit()
        
        return inserted_count

    def _get_table_count(self, schema: str, table: str) -> int:
        """Get total row count from a table."""
        try:
            with psycopg.connect(self._dsn, connect_timeout=self._connect_timeout_s) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT COUNT(*) FROM {};").format(_qual(schema, table))
                    )
                    result = cur.fetchone()
                    return result[0] if result else 0
        except Exception:
            return 0

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with psycopg.connect(self._dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    result = cur.fetchone()
                    return result is not None and result[0] == 1
        except Exception as e:
            _LOG.error("DB: Connection test failed: %s", e)
            return False


def fetch_new_ungated_rows() -> List[UngatedRow]:
    """Run the ungated ASINs query, store rows into the test table, and return them."""
    return DbService().fetch_new_ungated_rows()


def fetch_and_insert_new_ungated_rows() -> List[UngatedRow]:
    """Backward-compatible alias for fetch_new_ungated_rows()."""
    return fetch_new_ungated_rows()


def asins_from_rows(rows: Iterable[UngatedRow]) -> List[str]:
    """Extract ASINs from UngatedRow objects."""
    return [r.asin for r in rows if r.asin]


def upsert_normalized_csv_to_test_united_state(csv_path: Path) -> int:
    """Upsert normalized CSV data into the united_state table."""
    return DbService().upsert_normalized_csv_to_test_united_state(csv_path)