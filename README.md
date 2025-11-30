# SIMS Data Uploader

## Overview
SIMS Data Uploader is a toolkit for cleaning, validating, and ingesting school MIS spreadsheets into MariaDB. It is designed for
teams that routinely receive Excel workbooks with inconsistent schemas and need a repeatable way to normalize data for reporting
pipelines and future APIs. Recent work introduced a job-tracking subsystem so asynchronous services and UIs can surface upload
progress in real time.

## Key Capabilities
- **Excel normalization** – `app.prep_excel` rewrites messy worksheets into CSV files that match database staging tables while
  preserving metadata.
- **Bulk ingestion** – `app.ingest_excel` streams the normalized CSV into MariaDB using `LOAD DATA LOCAL INFILE`, filling audit
  columns such as `file_hash`, `batch_id`, `source_year`, and `ingested_at`.
- **Job tracking store** – `app.job_store` exposes helpers backed by the `upload_jobs`, `upload_job_events`, and
  `upload_job_results` tables to create jobs, record status transitions, and persist summary statistics.
- **Config-driven mappings** – `sql/sheet_ingest_config.sql` holds worksheet→table relationships, required columns, and options
  so onboarding new templates rarely requires Python changes.
- **Test coverage** – `tests/test_job_store.py` uses PyMySQL doubles to guarantee SQL statements, transactions, and JSON
  serialization behave as expected.

## Project Layout
```
sims-uploader/
├── app/
│   ├── __init__.py
│   ├── config.py            # Environment + database connection helpers
│   ├── job_store.py         # Dataclasses and SQL helpers for upload job tracking
│   ├── ingest_excel.py      # CLI to load normalized CSV into staging tables
│   └── prep_excel.py        # CLI/utility to sanitize Excel worksheets into CSV
│
├── sql/
│   ├── migrations/
│   │   └── 20241013_create_upload_job_tables.sql  # Upload job schema
│   ├── sheet_ingest_config.sql  # Configuration table for worksheet mappings
│   └── teach_record_raw.sql     # Example staging table DDL
│
├── tests/
│   ├── __init__.py
│   └── test_job_store.py        # Unit coverage for job store helpers
│
├── uploads/                # Workspace for inbound Excel/CSV files (gitignored)
├── requirements.txt        # Python dependencies
└── README.md
```

## Getting Started
### Prerequisites
- Ubuntu 22.04+ (or compatible), Python 3.10+, and access to MariaDB/MySQL 10.6+
- `python3 -m venv` for virtual environments
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
All CLI entrypoints read credentials through `app.config.get_db_settings()`, so keep secrets inside `.env` rather than source
files. Add Redis configuration, queue limits, and upload storage configuration alongside database credentials:

```
REDIS_URL=redis://localhost:6379/0
UPLOAD_QUEUE_NAME=sims_uploads         # optional override for the queue name
UPLOAD_MAX_FILE_SIZE_BYTES=104857600   # 100 MiB default; adjust per deployment
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
2. Apply migrations stored in `sql/migrations/` **in ascending filename order** to keep dependencies intact:
   ```sql
   SOURCE sql/migrations/20241013_create_upload_job_tables.sql;
   ```
   The initial migration introduces:
   - `upload_jobs` – a UUID-keyed record representing each uploaded workbook and its metadata.
   - `upload_job_events` – append-only log of state transitions (`status`, `message`, and timestamps).
   - `upload_job_results` – one-to-one summary metrics (row counts, normalized table name, rejected row path, JSON coverage).

   Run this migration immediately after the base schema when setting up a new environment, and reapply new migration files as they
   are added in future releases. A helper script is not required; relying on lexicographic filenames mirrors the approach used by
   MySQL clients such as `SOURCE` and keeps sequencing explicit.

## Command-Line Workflows
### 0. Queue Upload Jobs
```bash
python -m app.pipeline uploads/your_file.xlsx --source-year 2024
```
- Creates an entry in `upload_jobs`, enforces size/row limits, and enqueues the payload for background processing.
- Accepts the same worksheet and workbook type arguments as the legacy CLI but returns immediately with an RQ job id.

Start a worker in another terminal to drain the queue:

```bash
python -m app.job_runner worker
```
- Reads `REDIS_URL`/`UPLOAD_QUEUE_NAME` (or `--redis-url`/`--queue` overrides) to connect to Redis.
- Logs receipt of `SIGTERM`/`SIGINT` and requests a graceful shutdown after the current job finishes, ensuring the queue drains cleanly.

### 1. Preprocess Excel Workbooks
- ```bash
python -m app.prep_excel uploads/your_file.xlsx
```
- Cleans header rows, drops empty columns, and aligns column order with the target staging table.
- Hashes the workbook to prevent duplicate ingestion and appends the hash to the generated CSV filename.
- Validates presence of required columns declared in `sheet_ingest_config`. Missing headers raise a
  `MissingColumnsError` (or exit with code `2` when run via CLI) so calling services can surface actionable feedback.
- The CLI defaults to the `"default"` workbook type; ensure any workbook configuration intended for CLI use also inserts a
  matching `"default"` row (or always invoke the CLI with an explicit `--workbook-type`).

### 2. Bulk Load Normalized Data (Legacy)
```bash
python -m app.ingest_excel uploads/your_file.xlsx TEACH_RECORD --source-year 2024
```
- Skips work when the workbook hash already exists in the staging table.
- Loads the CSV through `LOAD DATA LOCAL INFILE`, populating metadata columns inside a single transaction.
- Accepts optional `--batch-id` and `--ingested-at` overrides for advanced scheduling workflows.
- Use this path for ad-hoc recovery; the recommended production flow is to queue jobs and let workers orchestrate ingestion.

### 3. Track Upload Progress
`app.job_store` exposes helpers that open short-lived PyMySQL connections using the same configuration as the CLI tools. The
new `app.job_runner.enqueue_job` helper wraps job creation + queueing when you need to schedule uploads from another service:
```python
import os

from app import job_runner

job_id, rq_job = job_runner.enqueue_job(
    workbook_path="uploads/your_file.xlsx",
    sheet="TEACH_RECORD",
    source_year="2024",
    file_size=os.path.getsize("uploads/your_file.xlsx"),
)
print("Queued job", job_id, "RQ id", rq_job.id)
```
- Every status change records an event row, making it easy to drive dashboards or alerting.
- Helpers return dataclasses populated from the database so timestamps, default values, and computed fields are readily available.
- Transactions automatically roll back on exceptions; integrity errors raise informative exceptions for the caller.

## Running Tests
```bash
pytest
```
The suite currently focuses on the job store to guarantee SQL parameters, transaction boundaries, and JSON serialization are
stable. Additional tests will be added as more ingest pipelines and APIs come online.

## API Service
- Start the FastAPI application with Uvicorn:
  ```bash
  uvicorn app.api:app --reload
  ```
- `POST /uploads/files` is the staging endpoint for raw workbook binaries. It validates the extension, enforces the file-size
  limit (`UPLOAD_MAX_FILE_SIZE_BYTES`), and persists the file under `UPLOAD_STORAGE_DIR` (default `./uploads`). The response
  includes the generated `stored_path`, which should be passed to `POST /uploads` as `workbook_path`.
- `POST /uploads` mirrors the CLI queue helper and enforces the same file-size/row-count hints before scheduling work.
- `GET /uploads/{job_id}`, `/uploads/{job_id}/events`, and `GET /uploads` expose the job store helpers for dashboards or CLI tooling to poll upload progress.

Browser and other interactive clients should upload files directly via `/uploads/files` instead of relying on server-visible
paths. The UI now performs this two-step process (file upload ➜ job enqueue) so operators do not need to pre-position
workbooks on disk.

## Operational Notes
- The repository ignores `.xlsx`, `.csv`, `.env`, and everything inside `uploads/`; never commit real student or teacher data.
- Always configure MariaDB with `utf8mb4` to handle Chinese characters, emoji, and future multilingual content.
- Ensure `local_infile` is enabled on both server and client connections so `LOAD DATA LOCAL INFILE` operates correctly.
- Rotate database credentials periodically and share `.env` values securely.
- Housekeeping: the upload storage directory (`UPLOAD_STORAGE_DIR` or `./uploads`) is append-only. Schedule periodic cleanup
  of staged files after they have been ingested to reclaim space. Size the volume based on `UPLOAD_MAX_FILE_SIZE_BYTES`,
  expected concurrency on `UPLOAD_QUEUE_NAME`, and the configured `UPLOAD_MAX_ROWS` so temporary storage does not exhaust the
  host.

## Roadmap
1. **Additional workbook types** – attendance, activities, awards, and counseling records with dedicated staging tables.
2. **FastAPI service** – browser-based uploader with progress dashboards powered by the job store.
3. **Automated normalization** – background workers to transform staging data into fully relational models (`students`,
   `teachers`, `subjects`, etc.).
4. **Observability** – structured logging, metrics, and alerting for ingestion failures.
5. **Containerization** – Docker images + compose files for consistent deployments.
6. **Data governance** – configurable retention policies for uploads, rejected rows, and job history.

## Contributing
1. Fork and clone the repository.
2. Create a virtual environment and install dependencies.
3. Run `pytest` before submitting pull requests.
4. Follow the lexicographic migration convention when adding new SQL files.
5. Document new workflows in this README so operations remain reproducible.
