import functools
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests

from .config import RocketSourceConfig
from .errors import ApiRequestError, ApiResponseError, ScanFailedError, ScanTimeoutError, ScanInProgressError, RateLimitError
from .utils import write_json_as_csv


_LOG = logging.getLogger(__name__)


def log_timing(name: str | None = None):
    """Decorator that logs execution timing at DEBUG level."""
    def decorator(func):
        """Wrap a function so its runtime is logged at DEBUG level."""
        label = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """Execute the wrapped function and emit a timing log line."""
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                _LOG.debug("%s took %.1fms", label, elapsed_ms)

        return wrapper

    return decorator


def get_retry_after(response: Optional[requests.Response]) -> int:
    """Extract Retry-After header value or return default wait time."""
    if response is None:
        return 30  # Default 30 seconds
    
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        try:
            # Could be seconds (integer) or HTTP-date
            return int(retry_after)
        except ValueError:
            # Try to parse HTTP-date format
            try:
                from email.utils import parsedate_to_datetime
                import datetime
                retry_time = parsedate_to_datetime(retry_after)
                now = datetime.datetime.now(datetime.timezone.utc)
                return max(1, int((retry_time - now).total_seconds()))
            except:
                pass
    return 30  # Default 30 seconds


def wrap_requests_errors():
    """Decorator that wraps requests exceptions into project-specific errors."""
    def decorator(func):
        """Wrap a function and convert requests exceptions into ApiRequestError."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """Execute the wrapped function with consistent request error handling."""
            try:
                return func(*args, **kwargs)
            except requests.HTTPError as e:
                resp = e.response
                method = None
                url = None
                status = None
                allow = None
                snippet = None

                if resp is not None:
                    status = resp.status_code
                    url = getattr(resp, "url", None)
                    allow = resp.headers.get("Allow")
                    if resp.request is not None:
                        method = getattr(resp.request, "method", None)
                    try:
                        text = (resp.text or "").strip()
                        if text:
                            snippet = text[:800]
                    except Exception:
                        snippet = None

                # Handle HTTP 429 specifically for scan in progress
                if status == 429:
                    # Try to extract scan in progress message
                    if resp and resp.text:
                        try:
                            error_data = resp.json()
                            if isinstance(error_data, dict) and error_data.get("message") == "You already have a scan in progress.":
                                # Re-raise as ScanInProgressError which can be caught separately
                                raise ScanInProgressError("A scan is already in progress. Please wait for it to complete.") from e
                        except:
                            pass
                    
                    # Generic rate limiting or concurrent scan limit
                    msg = "Too many requests or concurrent scan limit reached"
                    if method and url and status is not None:
                        msg = f"HTTP {status} {method} {url}"
                    elif status is not None and url:
                        msg = f"HTTP {status} {url}"
                    
                    _LOG.warning("%s - waiting before retry", msg)
                    raise RateLimitError(msg, retry_after=get_retry_after(resp)) from e
                    
                msg = f"HTTP error"
                if method and url and status is not None:
                    msg = f"HTTP {status} {method} {url}"
                elif status is not None and url:
                    msg = f"HTTP {status} {url}"

                if allow:
                    msg += f" (Allow: {allow})"
                if snippet:
                    msg += f" Response: {snippet}"

                _LOG.error("Request failed: %s", msg)
                raise ApiRequestError(msg) from e
            except requests.RequestException as e:
                _LOG.error("Request failed: %s", e)
                raise ApiRequestError(str(e)) from e
            except ScanInProgressError:
                # Re-raise ScanInProgressError without wrapping
                raise
            except RateLimitError:
                # Re-raise RateLimitError without wrapping
                raise
        return wrapper

    return decorator


class RocketSourceClient:
    """High-level client for RocketSource scan automation with concurrency handling."""
    def __init__(self, config: RocketSourceConfig, session: requests.Session | None = None) -> None:
        """Create a new client with the given config and optional requests session."""
        self._config = config
        self._log = logging.getLogger(self.__class__.__name__)
        self._session = session or requests.Session()
        self._max_retries = getattr(config, 'max_retries', 3)
        self._retry_delay = getattr(config, 'retry_delay', 30)

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def _url(self, path: str) -> str:
        """Build an absolute URL from the configured base_url and a path."""
        return self._config.base_url.rstrip("/") + "/" + path.lstrip("/")

    def _headers(self) -> dict[str, str]:
        """Build request headers including Authorization."""
        prefix = self._config.api_key_prefix
        value = f"{prefix}{self._config.api_key}" if prefix else self._config.api_key
        return {self._config.api_key_header: value, "Accept": "application/json"}

    def _json(self, resp: requests.Response) -> Any:
        """Parse response content as JSON."""
        try:
            return json.loads(resp.text)
        except Exception as e:
            raise ApiResponseError("Response is not valid JSON") from e

    def _extract_id_from_headers(self, resp: requests.Response) -> str | None:
        """Best-effort extraction of a scan/job/upload id from response headers."""
        # Some deployments return the scan id in a header (not the body).
        for k, v in resp.headers.items():
            lk = k.lower()
            if not isinstance(v, str) or not v.strip():
                continue
            if ("scan" in lk and "id" in lk) or ("job" in lk and "id" in lk) or ("upload" in lk and "id" in lk):
                return v.strip()
        return None

    def _list_scans(self, page: int = 1) -> Any:
        """Fetch a page of scans."""
        url = self._url("/scans")
        resp = self._session.get(url, headers=self._headers(), params={"page": page}, timeout=120)
        resp.raise_for_status()
        return self._json(resp)

    def _scan_items(self, scans_payload: Any) -> list[Any]:
        """Extract scan items from the list-scans response."""
        if isinstance(scans_payload, list):
            return scans_payload
        if isinstance(scans_payload, dict):
            data = scans_payload.get("data")
            if isinstance(data, list):
                return data
            items = scans_payload.get("scans")
            if isinstance(items, list):
                return items
        return []

    def _scan_name_from_item(self, item: Any) -> str | None:
        """Extract a scan name from a scan item."""
        if not isinstance(item, dict):
            return None
        v = item.get("name")
        if isinstance(v, str) and v.strip():
            return v.strip()
        opts = item.get("options")
        if isinstance(opts, dict):
            v2 = opts.get("name")
            if isinstance(v2, str) and v2.strip():
                return v2.strip()
        attrs = item.get("attributes")
        if isinstance(attrs, dict):
            opts2 = attrs.get("options")
            if isinstance(opts2, dict):
                v3 = opts2.get("name")
                if isinstance(v3, str) and v3.strip():
                    return v3.strip()
        return None

    def _extract_first(self, d: Any, keys: list[str]) -> str | None:
        """Recursively search for the first matching key within a JSON-like payload."""
        def _as_id(v: Any, key: str) -> str | None:
            """Coerce a JSON value to an id string while avoiding false positives."""
            if isinstance(v, str) and v.strip():
                return v
            if isinstance(v, (int, float)):
                # Avoid accidentally treating column indexes (0/1) as ids.
                if key == "id" and int(v) in (0, 1):
                    return None
                return str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
            return None

        def _walk(obj: Any, depth: int, parent_key: str | None = None) -> str | None:
            """Depth-limited walk of nested dict/list payloads to find an id field."""
            if depth > 6:
                return None
            if isinstance(obj, dict):
                for k in keys:
                    if k in obj:
                        got = _as_id(obj.get(k), k)
                        if got:
                            return got

                # Avoid scanning the attributes payload echoed back by the server (contains mapping.id=0).
                if parent_key == "mapping":
                    return None

                # Common API wrapper objects.
                for container in ("data", "scan", "job", "upload", "file", "result"):
                    if container in obj:
                        got = _walk(obj.get(container), depth + 1, container)
                        if got:
                            return got

                for k, v in obj.items():
                    if k in ("mapping",):
                        continue
                    got = _walk(v, depth + 1, k)
                    if got:
                        return got

            if isinstance(obj, list):
                for item in obj:
                    got = _walk(item, depth + 1, parent_key)
                    if got:
                        return got
            return None

        return _walk(d, 0)
    
    def check_existing_scans(self) -> list[dict[str, Any]]:
        """Check for existing scans that might be in progress."""
        try:
            scans_data = self._list_scans(page=1)
            scans = self._scan_items(scans_data)
            
            active_scans = []
            for scan in scans:
                status = self._extract_scan_status(scan)
                if status and status.lower() not in ["done", "completed", "failed", "error"]:
                    active_scans.append(scan)
            
            return active_scans
        except Exception as e:
            self._log.debug("Failed to check existing scans: %s", e)
            return []

    def _extract_scan_status(self, scan_item: Any) -> Optional[str]:
        """Extract status from a scan item."""
        if not isinstance(scan_item, dict):
            return None
        
        # Try different possible locations for status
        status = scan_item.get('status')
        if isinstance(status, str):
            return status
            
        attributes = scan_item.get('attributes', {})
        if isinstance(attributes, dict):
            status = attributes.get('status')
            if isinstance(status, str):
                return status
        
        data = scan_item.get('data', {})
        if isinstance(data, dict):
            status = data.get('status')
            if isinstance(status, str):
                return status
        
        return None

    @wrap_requests_errors()
    @log_timing(name="upload_csv")
    def upload_csv(self, csv_path: Path) -> str:
        """Upload a file and return an upload id (or scan id for API v3 /scans)."""
        return self._upload_csv_with_retry(csv_path)

    def _upload_csv_with_retry(self, csv_path: Path, retry_count: int = 0) -> str:
        """Internal method with retry logic for upload."""
        url = self._url(self._config.upload_path)
        
        try:
            with csv_path.open("rb") as f:
                files = {self._config.upload_file_field: (csv_path.name, f, "text/csv")}

                if self._config.upload_path.rstrip("/") == "/scans":
                    # RocketSource API v3: create scans via multipart upload to /scans.
                    # The request must include an "attributes" form field containing JSON.
                    try:
                        attrs = json.loads(self._config.scan_payload_template)
                    except Exception as e:
                        raise ApiResponseError("scan_payload_template is not valid JSON") from e

                    resp = self._session.post(
                        url,
                        headers=self._headers(),
                        files=files,
                        data={"attributes": json.dumps(attrs)},
                        timeout=120,
                    )
                else:
                    resp = self._session.post(url, headers=self._headers(), files=files, timeout=120)
            
            resp.raise_for_status()
            return self._extract_upload_id_from_response(resp)
            
        except RateLimitError as e:
            if retry_count < self._max_retries:
                wait_time = e.retry_after if hasattr(e, 'retry_after') else self._retry_delay
                self._log.warning("Rate limited. Waiting %d seconds before retry %d/%d", 
                                wait_time, retry_count + 1, self._max_retries)
                time.sleep(wait_time)
                return self._upload_csv_with_retry(csv_path, retry_count + 1)
            else:
                self._log.error("Max retries exceeded for upload")
                raise
        except ScanInProgressError:
            # Check if there's an existing scan we should wait for
            active_scans = self.check_existing_scans()
            if active_scans:
                self._log.info("Found %d active scan(s). You may need to wait for them to complete.", len(active_scans))
                # Optionally, you could poll the existing scan here
            raise

    def _extract_upload_id_from_response(self, resp: requests.Response) -> str:
        """Extract upload/scan ID from response."""
        data = self._json(resp)
        upload_id = self._extract_first(data, ["upload_id", "file_id", "id", "uploadId", "fileId", "scan_id", "scanId"])
        if not upload_id:
            header_id = self._extract_id_from_headers(resp)
            if header_id:
                return header_id

            location = resp.headers.get("Location") or resp.headers.get("location")
            if isinstance(location, str) and location.strip():
                try:
                    path = urlparse(location).path or location
                    parts = [p for p in path.split("/") if p]
                    if len(parts) >= 2 and parts[-2] == "scans":
                        return parts[-1]
                except Exception:
                    pass

            content_type = (resp.headers.get("Content-Type") or "").strip()
            snippet = (resp.text or "").strip()[:800]
            raise ApiResponseError(
                "Upload succeeded but upload_id was not found in response"
                + (f". Content-Type: {content_type}" if content_type else "")
                + (f". Location: {location}" if location else "")
                + (f". Response: {snippet}" if snippet else "")
            )
        return upload_id

    @wrap_requests_errors()
    @log_timing(name="create_scan")
    def create_scan(self, csv_path: Path) -> str:
        """Create a scan via API v3 (multipart POST /scans) and return the scan id."""
        # RocketSource API v3 scan creation happens in a single step (POST /scans).
        # Some deployments return only JSON string "ok" (no id), so we may need to
        # discover the new scan id by listing scans and comparing against a baseline.
        if self._config.upload_path.rstrip("/") != "/scans":
            return self.upload_csv(csv_path)

        try:
            attrs = json.loads(self._config.scan_payload_template)
        except Exception as e:
            raise ApiResponseError("scan_payload_template is not valid JSON") from e

        scan_name: str | None = None
        if isinstance(attrs, dict):
            opts = attrs.get("options")
            if isinstance(opts, dict) and isinstance(opts.get("name"), str):
                scan_name = opts.get("name")

        # Best-effort baseline so we can detect the new scan if the API returns only "ok".
        baseline_ids: set[str] = set()
        try:
            existing = self._list_scans(page=1)
            for item in self._scan_items(existing):
                sid = self._extract_first(item, ["id", "scan_id", "scanId"])
                if sid:
                    baseline_ids.add(sid)
        except Exception:
            baseline_ids = set()

        # Attempt to create scan with retry logic
        return self._create_scan_with_retry(csv_path, attrs, scan_name, baseline_ids)

    def _create_scan_with_retry(self, csv_path: Path, attrs: Any, scan_name: Optional[str], 
                               baseline_ids: set[str], retry_count: int = 0) -> str:
        """Internal method with retry logic for scan creation."""
        url = self._url("/scans")
        
        try:
            with csv_path.open("rb") as f:
                files = {self._config.upload_file_field: (csv_path.name, f, "text/csv")}
                resp = self._session.post(
                    url,
                    headers=self._headers(),
                    files=files,
                    data={"attributes": json.dumps(attrs)},
                    timeout=120,
                )
            resp.raise_for_status()

            # Process response
            return self._process_scan_creation_response(resp, scan_name, baseline_ids)
            
        except RateLimitError as e:
            if retry_count < self._max_retries:
                wait_time = e.retry_after if hasattr(e, 'retry_after') else self._retry_delay
                self._log.warning("Rate limited during scan creation. Waiting %d seconds before retry %d/%d", 
                                wait_time, retry_count + 1, self._max_retries)
                time.sleep(wait_time)
                return self._create_scan_with_retry(csv_path, attrs, scan_name, baseline_ids, retry_count + 1)
            else:
                self._log.error("Max retries exceeded for scan creation")
                raise
        except ScanInProgressError:
            active_scans = self.check_existing_scans()
            if active_scans:
                self._log.info("Cannot start new scan. Found %d active scan(s).", len(active_scans))
                for scan in active_scans:
                    scan_id = self._extract_first(scan, ["id", "scan_id", "scanId"])
                    status = self._extract_scan_status(scan)
                    self._log.info("  Scan %s: status=%s", scan_id, status)
            raise

    def _process_scan_creation_response(self, resp: requests.Response, scan_name: Optional[str], 
                                       baseline_ids: set[str]) -> str:
        """Process response from scan creation and extract scan ID."""
        header_id = self._extract_id_from_headers(resp)
        if header_id:
            return header_id

        try:
            data = self._json(resp)
        except Exception:
            data = None

        scan_id = self._extract_first(data, ["id", "scan_id", "scanId"]) if data is not None else None
        if scan_id:
            return scan_id

        # If no ID in response, poll for new scan
        return self._discover_scan_id_from_listing(scan_name, baseline_ids, resp)

    def _discover_scan_id_from_listing(self, scan_name: Optional[str], baseline_ids: set[str], 
                                      resp: requests.Response) -> str:
        """Poll scan listing to discover newly created scan."""
        start = time.time()
        attempt = 0

        while True:
            attempt += 1
            if time.time() - start > self._config.poll_timeout_s:
                content_type = (resp.headers.get("Content-Type") or "").strip()
                snippet = (resp.text or "").strip()[:800]
                raise ScanTimeoutError(
                    "Timed out waiting to discover scan id after upload"
                    + (f". Content-Type: {content_type}" if content_type else "")
                    + (f". Response: {snippet}" if snippet else "")
                )

            try:
                scans = self._list_scans(page=1)
            except Exception as e:
                self._log.debug("Failed to list scans during discovery: %s", e)
                time.sleep(self._config.poll_interval_s)
                continue

            for item in self._scan_items(scans):
                sid = self._extract_first(item, ["id", "scan_id", "scanId"])
                if not sid:
                    continue
                if baseline_ids and sid in baseline_ids:
                    continue
                if scan_name:
                    nm = self._scan_name_from_item(item)
                    if nm != scan_name:
                        continue
                return sid

            if attempt == 1 or attempt % 10 == 0:
                elapsed = time.time() - start
                self._log.info("Waiting to discover scan id... elapsed=%.0fs", elapsed)

            time.sleep(self._config.poll_interval_s)

    @wrap_requests_errors()
    @log_timing(name="start_scan")
    def start_scan(self, upload_id: str) -> str:
        """Legacy scan start call for APIs that separate upload and scan creation."""
        url = self._url(self._config.scan_path)
        payload_text = self._config.scan_payload_template.replace("{upload_id}", upload_id)
        try:
            payload = json.loads(payload_text)
        except Exception as e:
            raise ApiResponseError("scan_payload_template is not valid JSON") from e

        resp = self._session.post(
            url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()

        data = self._json(resp)
        scan_id = self._extract_first(data, ["scan_id", "job_id", "id", "scanId", "jobId"])
        if not scan_id:
            raise ApiResponseError("Scan start succeeded but scan_id was not found in response")
        return scan_id

    @wrap_requests_errors()
    @log_timing(name="poll_scan")
    def poll_scan(self, scan_id: str) -> str:
        """Poll scan status until completion/failure/timeout."""
        done_statuses = {"done", "completed", "complete", "finished", "success", "succeeded"}
        fail_statuses = {"failed", "error", "errored", "canceled", "cancelled"}

        start = time.time()
        last_status: str | None = None
        attempt = 0

        while True:
            attempt += 1
            if time.time() - start > self._config.poll_timeout_s:
                raise ScanTimeoutError(f"Timed out waiting for scan {scan_id}. last_status={last_status}")

            url = self._url(self._config.status_path_template.format(scan_id=scan_id))
            resp = self._session.get(url, headers=self._headers(), timeout=120)
            resp.raise_for_status()

            data = self._json(resp)
            status: str | None = None
            if isinstance(data, dict):
                v = data.get("status")
                if isinstance(v, str):
                    status = v
                elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("status"), str):
                    status = data["data"].get("status")
                elif (
                    isinstance(data.get("data"), dict)
                    and isinstance(data["data"].get("attributes"), dict)
                    and isinstance(data["data"]["attributes"].get("status"), str)
                ):
                    status = data["data"]["attributes"].get("status")
                elif isinstance(data.get("attributes"), dict) and isinstance(data["attributes"].get("status"), str):
                    status = data["attributes"].get("status")

                if status is None:
                    for k in ("state", "scan_status", "scanStatus"):
                        v2 = data.get(k)
                        if isinstance(v2, str) and v2.strip():
                            status = v2
                            break

            prev_status = last_status

            last_status = status
            if isinstance(status, str) and status.strip() and status != prev_status:
                elapsed = time.time() - start
                self._log.info("Scan %s status=%s (elapsed %.0fs)", scan_id, status, elapsed)
            elif isinstance(status, str) and status.strip() and (attempt % 10 == 0):
                elapsed = time.time() - start
                self._log.info("Scan %s still status=%s (elapsed %.0fs)", scan_id, status, elapsed)
            elif status is None and (attempt == 1 or attempt % 10 == 0):
                elapsed = time.time() - start
                self._log.info("Scan %s status not found yet (elapsed %.0fs)", scan_id, elapsed)
                if isinstance(data, dict):
                    self._log.debug("Status payload keys: %s", sorted([str(k) for k in data.keys()]))

            if isinstance(status, str):
                norm = status.lower().strip()
                if norm in done_statuses:
                    return status
                if norm in fail_statuses:
                    raise ScanFailedError(f"Scan {scan_id} failed: status={status}")

            time.sleep(self._config.poll_interval_s)

    @wrap_requests_errors()
    @log_timing(name="fetch_results")
    def fetch_results(self, scan_id: str) -> requests.Response:
        """Fetch scan results as a raw HTTP response."""
        url = self._url(self._config.results_path_template.format(scan_id=scan_id))
        if "/download" in self._config.results_path_template:
            # RocketSource exports use POST /scans/{scan_id}/download?type=csv|xlsx|json.
            resp = self._session.post(url, headers=self._headers(), timeout=300)
        else:
            resp = self._session.get(url, headers=self._headers(), timeout=300)
        resp.raise_for_status()
        return resp

    def save_results_csv(self, resp: requests.Response, out_path: Path) -> None:
        """Save results to CSV, converting JSON payloads to CSV if needed."""
        out_path.parent.mkdir(parents=True, exist_ok=True)
        content_type = (resp.headers.get("Content-Type") or "").lower()

        if "text/csv" in content_type or out_path.suffix.lower() == ".csv" and "application/json" not in content_type:
            out_path.write_bytes(resp.content)
            return

        data = self._json(resp)
        write_json_as_csv(data, out_path)

    def run_csv_scan(self, csv_path: Path, out_path: Path) -> tuple[str, str]:
        """Upload, poll, download, and save results; returns (upload_id, scan_id)."""
        # Check for existing scans before starting
        active_scans = self.check_existing_scans()
        if active_scans:
            self._log.warning("Found %d active scan(s) before starting new scan", len(active_scans))
            for scan in active_scans:
                scan_id = self._extract_first(scan, ["id", "scan_id", "scanId"])
                status = self._extract_scan_status(scan)
                self._log.warning("  Active scan %s: status=%s", scan_id, status)
        
        self._log.info("Starting scan for input=%s", csv_path)
        
        try:
            # API v3 default: create scan via /scans and treat returned id as scan id.
            # Legacy mode (upload + start_scan) is kept for older endpoint configurations.
            if self._config.upload_path.rstrip("/") == "/scans" and "{upload_id}" not in self._config.scan_payload_template:
                scan_id = self.create_scan(csv_path)
                upload_id = scan_id
            else:
                upload_id = self.upload_csv(csv_path)
                scan_id = self.start_scan(upload_id)
                
            self._log.info("Scan created. upload_id=%s scan_id=%s", upload_id, scan_id)
            self.poll_scan(scan_id)
            resp = self.fetch_results(scan_id)
            self.save_results_csv(resp, out_path)
            self._log.info("Results saved to %s", out_path)
            return upload_id, scan_id
            
        except ScanInProgressError as e:
            self._log.error("Cannot start scan: %s", str(e))
            self._log.info("Consider waiting for existing scans to complete or implementing queueing")
            raise
        except RateLimitError as e:
            self._log.error("Rate limited: %s", str(e))
            self._log.info("Consider implementing exponential backoff or queueing system")
            raise