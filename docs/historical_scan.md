# Historical Scan — Concept and Steps

This document describes the historical-scan process and how to perform it using the project's components. It intentionally does not reference or rely on any specific `historical_scan.py` script — the information here is conceptual and can be used to implement the process manually or via other automation in this repository.

## Goal

- **Run a single RocketSource CSV scan for a bounded set of ASINs taken from the database, retrieve the results, normalize them to the project's `united_state` format, and insert/update the database.**

## High-level steps

1. Select ASINs from the source table (Tirhak) — limit to N rows (controlled by `ROCKETSOURCE_ASIN_LIMIT`).
2. Optionally join or enrich those ASINs with seller or status data from the Umair table.
3. Build a CSV with header `ASIN,PRICE` and one row per ASIN (price can be a nominal value, e.g. `0.001`).
4. Upload the CSV to RocketSource using their API (either POST `/scans` or a two-step upload+start flow depending on deployment).
5. Poll the scan status until completion (done/completed/success etc.).
6. Download the results as CSV and persist them for inspection.
7. Normalize the RocketSource CSV into the project's `united_state` shape and upsert into the `united_state` table.

## Data selection (SQL concept)

- Recommended pattern: select up to `ROCKETSOURCE_ASIN_LIMIT` ASINs from the Tirhak table and LEFT JOIN Umair to attach optional seller/status information.
- Example conceptual SQL:

```
WITH tirhak_selected AS (
  SELECT asin, status FROM keepa_scrape.test_tirhak_gating LIMIT :asin_limit
)
SELECT t.asin, t.status as tirhak_status, u.status as umair_status, u.seller
FROM tirhak_selected t
LEFT JOIN keepa_scrape.test_umair_gating u ON t.asin = u.asin;
```

Notes:
- Use a sensible `LIMIT` (default: 1000) to keep scans compact and avoid rate limits.
- Choose ordering (oldest/newest) by adding an `ORDER BY` on an appropriate column if available.

## Building the CSV

- CSV header: `ASIN,PRICE`.
- Use `0.001` or another small number for the `PRICE` column unless you have real price data.
- The CSV can be built in-memory and uploaded directly — no need to write the file to disk.

## Uploading & scan creation

- RocketSource deployments may accept a multipart `POST /scans` call that returns a scan id, or they may return a generic `{ "ok" }` response and require you to discover the new scan id by listing scans.
- Implementations should:
  - POST the CSV and attributes payload to `/scans` (or upload then POST to `/start` for legacy APIs),
  - If the response contains an id, use it. If the response is generic (`"ok"`), poll the scans listing to find the newly created scan (compare against a baseline listing taken immediately before the upload).

## Polling

- Poll the scan status endpoint until the status falls into a completed set: e.g. `done`, `completed`, `complete`, `finished`, `success`, `succeeded`.
- Treat statuses like `failed`, `error`, `errored`, `canceled` as terminal failures.
- Use a poll interval (e.g. 5–30s) and a reasonable timeout (e.g. 30–60 minutes) depending on expected scan duration.

## Downloading results

- Download the CSV results from the scan results endpoint (may be `GET` or `POST` depending on endpoint template).
- Save a copy for inspection (recommended filename pattern: `historical_results_{scan_id}_{YYYYMMDD_HHMMSS}.csv`).

## Normalization

- Normalize RocketSource CSV columns into the `united_state` schema the project expects. Typical normalized columns:

  - `ASIN`
  - `US_BB_Price`
  - `Package_Weight`
  - `FBA_Fee`
  - `Referral_Fee`
  - `Shipping_Cost`
  - `Sales_Rank_Drops`
  - `Category`
  - `created_at`
  - `last_updated`
  - `Seller`

- Use consistent parsing for numeric and datetime columns and coerce empty/malformed values to safe defaults.

## Upsert into DB

- Insert/update rows into the `united_state` table using an upsert (INSERT ... ON CONFLICT DO UPDATE) so repeated runs update existing rows.

## Environment variables and configuration

- `DATABASE_URL` — Postgres connection string.
- `ROCKETSOURCE_BASE_URL` — RocketSource API base.
- `API_KEY` (and optionally `ROCKETSOURCE_API_KEY_HEADER` / prefix) — Authorization.
- `ROCKETSOURCE_ASIN_LIMIT` — number of ASINs to select from Tirhak (default 1000).
- `ROCKETSOURCE_DB_STATEMENT_TIMEOUT_MS`, `ROCKETSOURCE_DB_CONNECT_TIMEOUT_S` — DB behavior tuning.

## Troubleshooting

- DNS / connectivity: use `nslookup` / `Resolve-DnsName` / `Test-NetConnection` to verify DNS and port connectivity for Postgres.
- If API returns `429` or rate-limited responses, implement exponential backoff and/or reduce `ASIN_LIMIT`.
- If API returns a generic `"ok"` after upload, discover the scan id via scan listing and a baseline snapshot taken before upload.

## Safety and operations

- Avoid starting a new scan if another active scan exists; either wait for the active scan or coordinate with the RocketSource tenant operator.
- Keep scan sizes reasonable to avoid long-running scans and rate limiting.

## Next steps (implementation options)

- Manual flow: Run SQL to export ASINs, build a CSV, use an HTTP client to POST to RocketSource, poll, download, normalize, and upsert.
- Automated flow: Integrate the above steps into a small script or into existing automation in this repo; prefer building the CSV in-memory to avoid temporary files.

If you'd like, I can turn this into a `docs/README` section, add CLI examples, or produce a minimal runnable helper that implements these steps (without adding a committed script to the repo). Which would you prefer?
