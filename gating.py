import csv
import json
import hashlib
import hmac
import os
import random
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Tuple, List, Set
import threading
import psycopg
from psycopg import sql
import requests
from dotenv import load_dotenv

load_dotenv()

def _env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        v = os.getenv(name)
        if v is not None and v.strip() != "":
            return v
    return default

# ================== BASE CLIENT (from your code, trimmed) ==================
class ProductionSPAPI:
    def __init__(self):
        self.client_id = _env_first('PRODUCTION_CLIENT_ID', 'AMAZON_SP_API_LWA_CLIENT_ID')
        self.client_secret = _env_first('PRODUCTION_CLIENT_SECRET', 'AMAZON_SP_API_LWA_CLIENT_SECRET')
        self.refresh_token = _env_first('PRODUCTION_REFRESH_TOKEN', 'TIRHAK_REFRESH_TOKEN')

        self.endpoint = _env_first(
            'PRODUCTION_SP_API_ENDPOINT',
            'AMAZON_SP_API_ENDPOINT',
            default='https://sellingpartnerapi-na.amazon.com',
        )
        self.marketplace_id = os.getenv('AMAZON_MARKETPLACE_ID', 'ATVPDKIKX0DER')
        self.seller_id = _env_first('SELLER_ID', 'TIRHAK_SELLER_ID')  # REQUIRED

        self.aws_access_key = _env_first('AWS_ACCESS_KEY_ID', 'TIRHAK_AWS_ACCESS_KEY_ID')
        self.aws_secret_key = _env_first('AWS_SECRET_ACCESS_KEY', 'TIRHAK_AWS_SECRET_ACCESS_KEY')
        self.region = 'us-east-1'
        
        # Token caching
        self._access_token = None
        self._token_expiry = 0
        self._token_lock = threading.Lock()
        
        self._rate_lock = threading.Lock()
        self._next_request_at = 0.0
        
        self._validate_credentials()

    def _validate_credentials(self):
        missing = []
        if not self.client_id:
            missing.append('PRODUCTION_CLIENT_ID or AMAZON_SP_API_LWA_CLIENT_ID')
        if not self.client_secret:
            missing.append('PRODUCTION_CLIENT_SECRET or AMAZON_SP_API_LWA_CLIENT_SECRET')
        if not self.refresh_token:
            missing.append('PRODUCTION_REFRESH_TOKEN or TIRHAK_REFRESH_TOKEN')
        if not self.aws_access_key:
            missing.append('AWS_ACCESS_KEY_ID or TIRHAK_AWS_ACCESS_KEY_ID')
        if not self.aws_secret_key:
            missing.append('AWS_SECRET_ACCESS_KEY or TIRHAK_AWS_SECRET_ACCESS_KEY')
        if not self.seller_id:
            missing.append('SELLER_ID or TIRHAK_SELLER_ID')
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        print("Credentials validated")

    def get_access_token(self):
        with self._token_lock:
            # Return cached token if still valid (with 5-minute buffer)
            if self._access_token and time.time() < self._token_expiry - 300:
                return self._access_token
            
            # Fetch new token
            url = _env_first(
                "PRODUCTION_LWA_TOKEN_URL",
                "AMAZON_SP_API_TOKEN_URL",
                default="https://api.amazon.com/auth/o2/token",
            )
            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            r = requests.post(url, data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=60)
            r.raise_for_status()
            token_data = r.json()
            self._access_token = token_data['access_token']
            # Amazon tokens typically expire in 3600 seconds (1 hour)
            self._token_expiry = time.time() + token_data.get('expires_in', 3600)
            return self._access_token

    def _throttle(self):
        with self._rate_lock:
            now = time.time()
            if now < self._next_request_at:
                time.sleep(self._next_request_at - now)
            self._next_request_at = time.time() + BASE_DELAY

    def _sign_request(self, method, url, headers, payload):
        parsed = urllib.parse.urlparse(url)
        host, path, query = parsed.netloc, parsed.path, parsed.query
        t = datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')

        canonical_headers_dict = {'host': host, 'x-amz-date': amz_date}
        if 'x-amz-access-token' in headers:
            canonical_headers_dict['x-amz-access-token'] = headers['x-amz-access-token']
        sorted_headers = sorted(canonical_headers_dict.items())
        canonical_headers = '\n'.join([f'{k}:{v}' for k, v in sorted_headers]) + '\n'
        signed_headers = ';'.join([k for k, _ in sorted_headers])

        payload_hash = hashlib.sha256((payload or '').encode('utf-8')).hexdigest()
        canonical_request = f'{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}'

        algorithm = 'AWS4-HMAC-SHA256'
        scope = f'{date_stamp}/{self.region}/execute-api/aws4_request'
        string_to_sign = f'{algorithm}\n{amz_date}\n{scope}\n{hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

        k_date = sign(('AWS4' + self.aws_secret_key).encode('utf-8'), date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, 'execute-api')
        k_signing = sign(k_service, 'aws4_request')
        signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        headers['x-amz-date'] = amz_date
        headers['Authorization'] = f'{algorithm} Credential={self.aws_access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}'
        return headers

    def make_request(self, endpoint_path, method='GET', payload='', params=None):
        self._throttle()
        access_token = self.get_access_token()
        url = f"{self.endpoint}{endpoint_path}"
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)

        headers = {
            'x-amz-access-token': access_token,
            'Content-Type': 'application/json',
            'User-Agent': 'GatingChecker/1.1'
        }
        signed = self._sign_request(method, url, headers, payload or '')

        r = requests.get(url, headers=signed, timeout=60) if method == 'GET' else requests.post(url, headers=signed, data=payload, timeout=60)
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}", response=r)
        return r.json() if r.text.strip() else {}

    # ---- NEW: Listings Restrictions ----
    def get_listings_restrictions(self, asin: str, condition_type: str = "new_new"):
        # marketplaceIds expects a list/array format
        params = {
            "asin": asin,
            "sellerId": self.seller_id,
            "marketplaceIds": [self.marketplace_id],  # Pass as list for proper encoding
            "conditionType": condition_type
        }
        return self.make_request("/listings/2021-08-01/restrictions", method="GET", params=params)

# ================== GATING CLASSIFICATION ==================
def classify_restrictions(resp: Dict) -> Tuple[str, str, str]:
    """
    Returns (status, reason_code, approval_link)
    status: UNGATED | SOFT_GATED | HARD_GATED | NOT_FOUND | ERROR
    """
    try:
        restrictions = resp.get("restrictions", [])
        if not restrictions:
            return ("UNGATED", "", "")
        r = restrictions[0]
        reasons = r.get("reasons", [])
        if not reasons:
            return ("HARD_GATED", "UNKNOWN", "")
        reason = reasons[0]
        code = (reason.get("reasonCode") or "").upper()
        links = reason.get("links") or []
        link = links[0].get("resource") if links else ""
        if code == "APPROVAL_REQUIRED":
            return ("SOFT_GATED", code, link)
        if code == "NOT_ELIGIBLE":
            return ("HARD_GATED", code, "")
        if code == "ASIN_NOT_FOUND":
            return ("NOT_FOUND", code, "")
        return ("HARD_GATED", code or "UNKNOWN", link)
    except Exception as e:
        return ("ERROR", f"PARSE_ERROR:{e}", "")

# ================== CSV/JSON PIPELINE ==================
# ⚙️ CONFIGURE YOUR INPUT FILE HERE ⚙️
DB_SCHEMA = os.getenv("DB_SCHEMA", "keepa_scrape")
DB_TABLE = os.getenv("DB_TABLE", "Downloaded_asin")
GATING_DB_SCHEMA = os.getenv("GATING_DB_SCHEMA", DB_SCHEMA)
GATING_DB_TABLE = os.getenv("GATING_DB_TABLE", "test_Tirhak_gating status")
try:
    GATING_INPUT_LIMIT = int(float(os.getenv("GATING_INPUT_LIMIT", "10")))
except Exception:
    GATING_INPUT_LIMIT = 10
try:
    GATING_DB_BATCH_SIZE = int(float(os.getenv("GATING_DB_BATCH_SIZE", "200")))
except Exception:
    GATING_DB_BATCH_SIZE = 200

OUTPUT_CSV = os.getenv("ASIN_OUTPUT_CSV", "")
CONDITION  = os.getenv("ASIN_CONDITION", "new_new")

# default ~4.5 req/sec to respect 5 rps limit; adjust if you have higher quota
BASE_DELAY = float(os.getenv("REQ_DELAY_SECONDS", "0.22"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Concurrent threads

def _db_url():
    for key in ("ROCKETSOURCE_DB_URL", "DATABASE_URL", "DB_URL", "POSTGRES_URL"):
        v = os.environ.get(key)
        if v and v.strip():
            return v.strip()

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    if db_name and db_user and db_password and db_host and db_port:
        return f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"

    raise ValueError("Missing database URL. Set ROCKETSOURCE_DB_URL (or DATABASE_URL/DB_URL/POSTGRES_URL).")

def _ensure_gating_table(cur) -> None:
    cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(GATING_DB_SCHEMA)))
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                asin VARCHAR(20) PRIMARY KEY,
                status VARCHAR(32),
                reason_code VARCHAR(128),
                approval_link TEXT,
                last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
        ).format(sql.Identifier(GATING_DB_SCHEMA), sql.Identifier(GATING_DB_TABLE))
    )

    cur.execute(
        sql.SQL("ALTER TABLE {}.{} DROP COLUMN IF EXISTS created_at;").format(
            sql.Identifier(GATING_DB_SCHEMA),
            sql.Identifier(GATING_DB_TABLE),
        )
    )

def _fetch_input_asins(cur) -> List[str]:
    base = sql.SQL(
        """
        SELECT DISTINCT UPPER(TRIM(d.asin)) AS asin
        FROM {}.{} d
        LEFT JOIN {}.{} o
          ON UPPER(TRIM(o.asin)) = UPPER(TRIM(d.asin))
        WHERE d.asin IS NOT NULL
          AND o.asin IS NULL
        ORDER BY asin
        """
    ).format(
        sql.Identifier(DB_SCHEMA),
        sql.Identifier(DB_TABLE),
        sql.Identifier(GATING_DB_SCHEMA),
        sql.Identifier(GATING_DB_TABLE),
    )

    if GATING_INPUT_LIMIT and GATING_INPUT_LIMIT > 0:
        cur.execute(base + sql.SQL(" LIMIT %s"), (GATING_INPUT_LIMIT,))
    else:
        cur.execute(base)

    asins: List[str] = []
    for (asin,) in cur.fetchall():
        a = (asin or "").strip().upper()
        if a:
            asins.append(a)
    return asins

def _upsert_gating_rows(cur, rows: List[dict]) -> None:
    if not rows:
        return
    q = sql.SQL(
        """
        INSERT INTO {}.{} (asin, status, reason_code, approval_link, last_updated)
        VALUES (%(asin)s, %(status)s, %(reason_code)s, %(approval_link)s, CURRENT_TIMESTAMP)
        ON CONFLICT (asin) DO UPDATE
        SET
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            approval_link = EXCLUDED.approval_link,
            last_updated = CURRENT_TIMESTAMP;
        """
    ).format(sql.Identifier(GATING_DB_SCHEMA), sql.Identifier(GATING_DB_TABLE))
    cur.executemany(q, rows)

def process_single_asin(api: ProductionSPAPI, asin: str) -> Tuple[str, str, str, str]:
    """Process a single ASIN with retry logic. Returns (asin, status, reason_code, approval_link)"""
    attempt = 0
    while True:
        try:
            resp = api.get_listings_restrictions(asin=asin, condition_type=CONDITION)
            status, code, link = classify_restrictions(resp)
            return (asin, status, code, link)
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            # Retry on throttling or transient server errors
            if status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                attempt += 1
                backoff_sleep(BASE_DELAY, attempt)
                continue
            # Permanent error: return error status
            return (asin, "ERROR", f"HTTP_{status_code}", "")
        except Exception as e:
            if attempt < MAX_RETRIES:
                attempt += 1
                backoff_sleep(BASE_DELAY, attempt)
                continue
            return (asin, "ERROR", type(e).__name__, "")

def backoff_sleep(base_delay: float, attempt: int):
    # exponential backoff with jitter
    sleep_s = min(10.0, (2 ** attempt) * base_delay) + random.uniform(0, base_delay)
    time.sleep(sleep_s)

def write_header_if_needed(path: str):
    if not path or not path.strip():
        return
    if not os.path.exists(path):
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["asin", "status", "reason_code", "approval_link"])
            writer.writeheader()

def process_csv():
    start_time = time.time()
    api = ProductionSPAPI()

    dsn = _db_url()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_gating_table(cur)
            conn.commit()
            to_do = _fetch_input_asins(cur)

    print(f"\n{'='*60}")
    print(f"📥 Input Table: {DB_SCHEMA}.{DB_TABLE}")
    print(f"💾 Output Table: {GATING_DB_SCHEMA}.{GATING_DB_TABLE}")
    print(f"🚀 To check now: {len(to_do)}")
    print(f"⚙️  Max concurrent workers: {MAX_WORKERS}")
    print(f"⏱️  Rate limit delay: {BASE_DELAY}s per request")
    print(f"{'='*60}\n")
    
    if not to_do:
        print("✅ All ASINs already processed!")
        return

    csv_enabled = isinstance(OUTPUT_CSV, str) and OUTPUT_CSV.strip() != ""
    if csv_enabled:
        write_header_if_needed(OUTPUT_CSV)
    
    completed_count = 0

    buffer: List[dict] = []

    if csv_enabled:
        f_out = open(OUTPUT_CSV, 'a', newline='', encoding='utf-8')
        writer = csv.DictWriter(f_out, fieldnames=["asin","status","reason_code","approval_link"])
    else:
        f_out = None
        writer = None

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                _ensure_gating_table(cur)
                conn.commit()

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_asin = {executor.submit(process_single_asin, api, asin): asin for asin in to_do}

                    for future in as_completed(future_to_asin):
                        asin, status, code, link = future.result()

                        if writer is not None:
                            writer.writerow({"asin": asin, "status": status, "reason_code": code, "approval_link": link})
                            if f_out is not None:
                                f_out.flush()

                        buffer.append({"asin": asin, "status": status, "reason_code": code, "approval_link": link})
                        completed_count += 1

                        if len(buffer) >= GATING_DB_BATCH_SIZE:
                            _upsert_gating_rows(cur, buffer)
                            conn.commit()
                            buffer.clear()

                        elapsed = time.time() - start_time
                        rate = completed_count / elapsed if elapsed > 0 else 0
                        remaining = len(to_do) - completed_count
                        eta_seconds = remaining / rate if rate > 0 else 0
                        eta_min = int(eta_seconds / 60)

                        print(
                            f"[{completed_count}/{len(to_do)}] {asin}: {status}{' ('+code+')' if code else ''} | "
                            f"Rate: {rate:.2f}/s | ETA: {eta_min}m {int(eta_seconds % 60)}s"
                        )

                if buffer:
                    _upsert_gating_rows(cur, buffer)
                    conn.commit()
                    buffer.clear()
    finally:
        if f_out is not None:
            f_out.close()

    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ Finished processing {len(to_do)} ASINs")
    print(f"⏱️  Total runtime: {int(elapsed_total / 60)}m {int(elapsed_total % 60)}s")
    print(f"📊 Average rate: {len(to_do) / elapsed_total:.2f} ASINs/second")
    if csv_enabled:
        print(f"💾 Results saved to: {OUTPUT_CSV}")
    else:
        print("💾 CSV output disabled")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    process_csv()