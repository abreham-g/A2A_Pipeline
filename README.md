# RocketSource CSV Scan Automation

Automate RocketSource CSV uploads and scans from the command line.

This project provides a small Python CLI that:

- Uploads a supplier CSV/XLSX file to RocketSource (API v3)
- Waits for the scan to complete
- Downloads the results as CSV

## Requirements

- Python 3.10+ (recommended)
- A RocketSource API key (from **Integrations** in the RocketSource web app)

## Setup

1) Create and activate a virtual environment (recommended).

2) Install dependencies:

```powershell
py -m pip install -r requirements.txt
```

3) Configure environment variables

Copy the example env file and fill in your API key:

```powershell
copy .env.example .env
```

Then edit `.env`.

Minimum required variables:

- `ROCKETSOURCE_BASE_URL`
- `ROCKETSOURCE_API_KEY`

Recommended defaults for RocketSource API v3:

```env
ROCKETSOURCE_BASE_URL="https://app.rocketsource.io/api/v3"
ROCKETSOURCE_API_KEY="YOUR_API_KEY_HERE"
```

Authentication uses:

- Header: `Authorization`
- Value: `Bearer <TOKEN>`

(These can be customized via `ROCKETSOURCE_API_KEY_HEADER` and `ROCKETSOURCE_API_KEY_PREFIX` if needed.)

## Usage

### Run a scan from the CLI

From the project root:

```powershell
py -m Script.cli Data\test.csv --out out.csv
```

### Run a scan using database input

If you have a Postgres connection URL in your `.env`, you can run the end-to-end flow that:

- Executes the ungated ASIN query
- Selects ASINs that are not yet present in `"Core Data"."avg_book_sports_cd_tools_toys_ungated"` (currently `LIMIT 10`)
- Generates a temporary `ASIN,PRICE` CSV under `Data/`
- Runs the RocketSource scan using that generated CSV

Set one of the following environment variables:

- `ROCKETSOURCE_DB_URL` (preferred)
- `DATABASE_URL` / `DB_URL` / `POSTGRES_URL` (fallback)

Then run:

```powershell
py rocketsource_automation.py --out out.csv
```

Notes:

- Input CSV paths are resolved relative to the `Data/` folder if you pass a simple filename.
- Output is saved under `Data/` unless you pass an absolute output path.

### Common options

```powershell
py -m Script.cli Data\test.csv --out out.csv --log-level DEBUG
```

Polling controls:

```powershell
py -m Script.cli Data\test.csv --out out.csv --interval 5 --timeout 600
```

### Customizing the scan payload (column mapping, scan name, marketplace)

RocketSource API v3 creates a scan via `POST /scans` with a multipart upload containing:

- `file` (CSV/XLSX)
- `attributes` (JSON string)

You can override the default `attributes` payload using `ROCKETSOURCE_SCAN_PAYLOAD`.

Example:

```env
ROCKETSOURCE_SCAN_PAYLOAD='{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"My Automated Scan"}}'
```

## Running tests

Run tests from the project root (do not run the test files directly):

```powershell
py -m pytest -q
```

## Docker

This project can be run in Docker without changing the Python code.

Prerequisites:

- Docker Desktop
- A `.env` file configured with `ROCKETSOURCE_BASE_URL`, `ROCKETSOURCE_API_KEY`, and your DB URL (for DB mode)

Build and run the automation:

```powershell
docker compose build
docker compose run --rm rocketsource
```

Outputs are written to `Data/` on your host via a bind mount.

Run tests in Docker:

```powershell
docker compose --profile test run --rm tests
```

## Troubleshooting

### HTTP 422 during upload

If you see `HTTP 422` from `/api/v3/scans`, your `ROCKETSOURCE_SCAN_PAYLOAD` is not matching the current API schema.
Start with the minimal payload:

```json
{"mapping":{"id":0,"cost":1},"options":{"marketplace_id":"US","name":"Automated Scan"}}
```

### Upload returns body "ok" and no scan id

Some deployments respond with JSON string `"ok"` instead of returning the scan id in the response body.
The client handles this by listing scans and detecting the newly created scan.

## Project layout

- `Script/cli.py` - CLI entrypoint
- `Script/client.py` - RocketSource API client (upload/poll/download)
- `Script/config.py` - configuration (env vars)
- `Script/errors.py` - typed exceptions
- `Script/utils.py` - helpers (JSON-to-CSV fallback)
- `test/` - pytest unit tests




