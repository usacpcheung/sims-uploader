# SIMS Data Uploader

## Overview
SIMS Data Uploader converts messy SMIS spreadsheets into normalized MariaDB tables while tracking every upload as a background job. The toolkit includes:

- A FastAPI service with REST endpoints for uploading binaries, enqueuing jobs, and polling progress.
- A thin web UI (Jinja templates + static JS) that mirrors the API contract described in [`docs/web-ui.md`](docs/web-ui.md).
- CLI utilities for local normalization, bulk loading, and full-pipeline orchestration.
- A Redis/RQ worker that executes the end-to-end pipeline and records validation errors.

Recent work added an end-to-end pipeline (`app.pipeline`) that chains worksheet preparation, staging loads, normalized schema management, validation, and rejected-row export. Both the API and CLI share this pipeline so behavior stays consistent across interfaces.

## Key Capabilities
- **Worksheet normalization** – `app.prep_excel` cleans headers, drops empty columns, and emits CSV aligned with staging tables.
- **Staging + normalization pipeline** – `app.pipeline.run_pipeline` orchestrates staging via `app.ingest_excel`, normalized schema management via `app.normalize_staging`, and row-level validation via `app.validation`. Validation errors get surfaced to callers and saved alongside rejected rows.
- **Job tracking store** – `app.job_store` manages `upload_jobs`, `upload_job_events`, and `upload_job_results` tables to expose status, event history, and summary metrics.
- **Queued execution** – `app.job_runner` registers jobs with Redis/RQ, enforces file-size/row-count limits, and marks status transitions during worker execution.
- **Storage helpers** – `app.storage` enforces Excel extensions, stores binaries under a UUID-prefixed path, and recovers the original filename for database records.
- **API + UI** – `app.api` exposes `/uploads/files`, `/uploads`, `/uploads/{id}`, and `/uploads/{id}/events`. `app.web` serves a starter UI at `/ui` for operators who prefer a browser.

## Project Layout
```
sims-uploader/
├── app/
│   ├── api.py                # FastAPI endpoints for uploads, job detail, and events
│   ├── config.py             # Environment + database connection helpers
│   ├── ingest_excel.py       # Bulk load normalized CSV into staging tables
│   ├── job_runner.py         # RQ queue + worker utilities and job status hooks
│   ├── job_store.py          # Dataclasses and SQL helpers for job/event/result tables
│   ├── normalize_staging.py  # Normalize staging rows into flexible destination schemas
│   ├── pipeline.py           # End-to-end prep → staging → normalization → validation
│   ├── prep_excel.py         # CLI/utility to sanitize Excel worksheets into CSV
│   ├── storage.py            # File-extension enforcement and upload path helpers
│   ├── validation.py         # Validation + rejected-row export for normalized rows
│   └── web.py                # Jinja routes for the browser-based uploader
│
├── docs/
│   └── web-ui.md             # Contract between the web UI and the Upload API
├── sql/
│   ├── migrations/
│   │   └── 20241013_create_upload_job_tables.sql
│   ├── sheet_ingest_config.sql
│   └── teach_record_raw.sql
├── static/                   # JS/CSS assets referenced by the Jinja templates
├── templates/                # HTML templates for the built-in web UI
├── tests/
│   └── test_job_store.py     # Unit coverage for job store helpers
├── uploads/                  # Workspace for inbound Excel/CSV files (gitignored)
├── requirements.txt          # Python dependencies
└── README.md
```

## Getting Started
### Prerequisites
- Ubuntu 22.04+ (or compatible), Python 3.10+, and access to MariaDB/MySQL 10.6+
- Redis + RQ (for queued pipelines)
- Database account with privileges to create schemas, tables, indexes, and run `LOAD DATA LOCAL INFILE`

### Installation
```bash
git clone git@github.com:usacpcheung/sims-uploader.git
cd sims-uploader
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # populate DB host/user/password/database + Redis settings
mkdir -p uploads      # optional workspace for raw spreadsheets
```
All entrypoints load credentials via `app.config.get_db_settings()`, so keep secrets inside `.env` rather than source files. Add Redis configuration, queue limits, and upload storage configuration alongside database credentials:

```env
REDIS_URL=redis://localhost:6379/0
UPLOAD_QUEUE_NAME=sims_uploads         # override queue name if desired
UPLOAD_MAX_FILE_SIZE_BYTES=104857600   # 100 MiB default; override per deployment
UPLOAD_MAX_ROWS=500000                 # Reject uploads reporting more rows than this limit
UPLOAD_STORAGE_DIR=/var/lib/sims-uploads  # Optional; defaults to ./uploads when unset
```

## Database Setup
1. Create the schema and source the base SQL scripts:
   ```sql
   CREATE DATABASE SIMSdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   USE SIMSdata;
   SOURCE sql/sheet_ingest_config.sql;
   SOURCE sql/teach_record_raw.sql;
   ```
2. Apply migrations in `sql/migrations/` in ascending filename order so dependencies remain intact:
   ```sql
   SOURCE sql/migrations/20241013_create_upload_job_tables.sql;
   ```
   The migration introduces `upload_jobs` (job metadata), `upload_job_events` (status history), and `upload_job_results` (aggregate counts + coverage metadata).

## Command-Line Workflows
### 0. Queue Upload Jobs (recommended)
```bash
python -m app.pipeline uploads/your_file.xlsx --source-year 2024
```
- Creates a job row and enqueues the payload for background processing.
- Enforces file-size/row-count limits before the worker starts.

Start a worker in another terminal to drain the queue:
```bash
python -m app.job_runner worker
```
- Honors `REDIS_URL`/`UPLOAD_QUEUE_NAME` (or `--redis-url`/`--queue` overrides).
- Logs SIGTERM/SIGINT and drains the current job before shutdown.

### 1. Preprocess Excel Workbooks
```bash
python -m app.prep_excel uploads/your_file.xlsx
```
- Cleans headers, drops empty columns, and aligns column order with the target staging table.
- Hashes the workbook to prevent duplicate ingestion and appends the hash to the generated CSV filename.
- Validates required columns from `sheet_ingest_config`; missing headers raise `MissingColumnsError` (CLI exit code `2`).

### 2. Bulk Load Normalized Data (legacy/manual)
```bash
python -m app.ingest_excel uploads/your_file.xlsx TEACH_RECORD --source-year 2024
```
- Skips work when the workbook hash already exists in the staging table.
- Uses `LOAD DATA LOCAL INFILE` inside a transaction to populate metadata columns.
- Accepts optional `--batch-id` and `--ingested-at` overrides for recovery workflows.

### 3. Full Pipeline Execution (worker path)
The worker path wraps preparation, staging, normalization, and validation:
- `app.pipeline.run_pipeline` prepares the worksheet, loads staging rows, ensures the normalized table exists, validates rows, inserts normalized data, exports rejected rows, and records coverage metadata.
- Status updates (`Queued` → `Parsing` → `Validating` → `Loaded`/`Errors`) are captured in `upload_job_events`.
- Validation failures mark the job as `Errors` and surface a summary in both the API and UI.

### 4. Plan new workbook ingest configuration
```bash
python app/ingest_planner.py uploads/your_file.xlsx --emit-sql --workbook-type <type>
```
- Scans each sheet to capture cleaned headers, suggested staging columns, metadata columns, and inferred column types.
- Produces a JSON plan next to the workbook (override with `-o <path>`) and, with `--emit-sql`, writes a `*_sheet_ingest_config.sql` snippet for review before updating [`sql/sheet_ingest_config.sql`](sql/sheet_ingest_config.sql).
- Table names can be templated via `--staging-table-template`/`--normalized-table-template` and column types can be overridden with `--column-type-override COL:TYPE` when the defaults are not suitable.
- `sheet_ingest_config` rows also accept optional `time_range_column`, `time_range_format`, and `overlap_target_table` values to describe how to interpret workbook date spans and where to compare them when detecting overlaps.

## API + Web UI
Start the FastAPI service with Uvicorn:
```bash
uvicorn app.api:app --reload
```
- `POST /uploads/files` streams the workbook binary into `UPLOAD_STORAGE_DIR`, enforcing extension and size checks.
- `POST /uploads` enqueues the upload job (mirrors the CLI queue helper) and returns the created job.
- `GET /uploads/{job_id}` and `GET /uploads/{job_id}/events` provide job detail and event timelines.
- `GET /uploads` lists recent jobs (default 20, max 100) newest first.
- Browser users can visit `/ui` for a basic uploader built on these endpoints; see [`docs/web-ui.md`](docs/web-ui.md) for the UI contract and polling cadence.

## Configuration Notes
- `.xlsx`, `.xlsm`, and `.xls` are accepted; other extensions are rejected at upload time.
- `UPLOAD_STORAGE_DIR` is created automatically; size it based on `UPLOAD_MAX_FILE_SIZE_BYTES` and expected concurrency.
- `uploads/` is gitignored; never commit real student or teacher data.
- Ensure MariaDB `local_infile` is enabled for `LOAD DATA LOCAL INFILE` to operate correctly.

## Running Tests
```bash
pytest
```
The current suite focuses on the job store to verify SQL parameters, transaction boundaries, and JSON serialization. Add targeted tests alongside new ingest pipelines, validators, and API routes.

## Roadmap
1. Harden validation and rejected-row reporting for additional workbook types (attendance, activities, awards, counseling records).
2. Expand API filtering/pagination for job listings and event timelines to support deeper history views in the UI.
3. Add structured logging/metrics and container images for production deployments.
4. Automate normalization into relational models (`students`, `teachers`, `subjects`, etc.) after staging ingestion completes.
