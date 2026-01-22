#!/usr/bin/env python3
"""
Import ASINs from CSV file to PostgreSQL database.

This script reads ASINs from a CSV file and stores them in the specified
database table with proper schema creation and error handling.
"""

import csv
import os
import sys
import time
from pathlib import Path
from typing import List, Optional
from datetime import datetime

import psycopg
import psycopg.sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_URL = os.getenv("DATABASE_URL") or os.getenv("ROCKETSOURCE_DB_URL")
if not DB_URL:
    print("ERROR: DATABASE_URL or ROCKETSOURCE_DB_URL not found in environment")
    sys.exit(1)

# Target schema and table
TARGET_SCHEMA = "api_scraper"
TARGET_TABLE = "tirhak_and_umair_25k"

# Logging
def log_info(message: str):
    """Print info message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def log_error(message: str):
    """Print error message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {message}", file=sys.stderr)

def read_asins_from_csv(csv_path: Path) -> List[str]:
    """Read ASINs from CSV file."""
    asins = []
    try:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # Skip header if present
            header = next(reader, None)
            if header and header[0].strip().upper() == "ASIN":
                log_info("Found ASIN header, skipping first row")
            else:
                # If no header, rewind to start
                f.seek(0)
                reader = csv.reader(f)
            
            for row_num, row in enumerate(reader, start=1):
                if not row or not row[0].strip():
                    continue
                asin = row[0].strip()
                if asin:
                    asins.append(asin)
        
        log_info(f"Read {len(asins)} ASINs from {csv_path}")
        return asins
    
    except Exception as e:
        log_error(f"Failed to read CSV file {csv_path}: {e}")
        raise

def create_schema_and_table(conn):
    """Create schema and table if they don't exist."""
    with conn.cursor() as cur:
        # Create schema if it doesn't exist
        cur.execute(psycopg.sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
            psycopg.sql.Identifier(TARGET_SCHEMA)
        ))
        log_info(f"Ensured schema '{TARGET_SCHEMA}' exists")
        
        # Create table if it doesn't exist
        create_table_sql = psycopg.sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                asin VARCHAR(20) PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS {} ON {} (created_at);
        """).format(
            psycopg.sql.Identifier(TARGET_SCHEMA, TARGET_TABLE),
            psycopg.sql.Identifier(f"idx_{TARGET_TABLE}_created_at"),
            psycopg.sql.Identifier(TARGET_SCHEMA, TARGET_TABLE)
        )
        
        cur.execute(create_table_sql)
        log_info(f"Ensured table '{TARGET_SCHEMA}.{TARGET_TABLE}' exists")
        
        conn.commit()

def import_asins_to_db(asins: List[str], source_file: str) -> int:
    """Import ASINs to database table."""
    if not asins:
        log_info("No ASINs to import")
        return 0
    
    imported_count = 0
    skipped_count = 0
    
    try:
        with psycopg.connect(DB_URL, connect_timeout=10) as conn:
            log_info(f"Connected to database: {DB_URL.split('@')[1] if '@' in DB_URL else 'unknown'}")
            
            # Create schema and table
            create_schema_and_table(conn)
            
            # Prepare insert statement with ON CONFLICT handling
            insert_sql = psycopg.sql.SQL("""
                INSERT INTO {} (asin, updated_at)
                VALUES (%s, CURRENT_TIMESTAMP)
                ON CONFLICT (asin) 
                DO UPDATE SET 
                    updated_at = CURRENT_TIMESTAMP
                RETURNING asin;
            """).format(psycopg.sql.Identifier(TARGET_SCHEMA, TARGET_TABLE))
            
            # Batch insert ASINs
            batch_size = 1000
            total_batches = (len(asins) + batch_size - 1) // batch_size
            
            log_info(f"Importing {len(asins)} ASINs in {total_batches} batches of {batch_size}")
            
            start_time = time.time()
            
            with conn.cursor() as cur:
                for i in range(0, len(asins), batch_size):
                    batch = asins[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    try:
                        # Execute batch insert
                        results = cur.executemany(insert_sql, [(asin,) for asin in batch])
                        
                        # Count successful inserts (new records)
                        batch_imported = len(results.fetchall()) if hasattr(results, 'fetchall') else len(batch)
                        imported_count += batch_imported
                        
                        log_info(f"Batch {batch_num}/{total_batches}: {batch_imported} new, {len(batch) - batch_imported} updated")
                        
                    except Exception as e:
                        log_error(f"Failed to insert batch {batch_num}: {e}")
                        # Try individual inserts for this batch
                        for asin in batch:
                            try:
                                cur.execute(insert_sql, (asin,))
                                result = cur.fetchone()
                                if result:
                                    imported_count += 1
                                else:
                                    skipped_count += 1
                            except Exception as individual_error:
                                log_error(f"Failed to insert ASIN {asin}: {individual_error}")
                                skipped_count += 1
                    
                    # Commit every batch to avoid large transactions
                    conn.commit()
            
            total_time = time.time() - start_time
            log_info(f"Import completed in {total_time:.1f}s")
            
    except Exception as e:
        log_error(f"Database operation failed: {e}")
        raise
    
    return imported_count

def main(csv_file: str = "test_Asins.csv"):
    """Main import function."""
    # Determine CSV file path
    data_dir = Path(__file__).parent / "Data"
    csv_path = data_dir / csv_file
    
    if not csv_path.exists():
        log_error(f"CSV file not found: {csv_path}")
        return 1
    
    log_info(f"Starting ASIN import from {csv_path}")
    log_info(f"Target: {TARGET_SCHEMA}.{TARGET_TABLE}")
    
    try:
        # Read ASINs from CSV
        asins = read_asins_from_csv(csv_path)
        
        if not asins:
            log_info("No ASINs found in CSV file")
            return 0
        
        # Import to database
        imported_count = import_asins_to_db(asins, str(csv_path))
        
        log_info(f"Successfully imported {imported_count} new ASINs")
        log_info(f"Total ASINs processed: {len(asins)}")
        
        return 0
        
    except Exception as e:
        log_error(f"Import failed: {e}")
        return 1

if __name__ == "__main__":
    # Parse command line arguments
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "test_Asins.csv"
    
    exit_code = main(csv_file)
    sys.exit(exit_code)
