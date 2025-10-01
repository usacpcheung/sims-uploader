# SIMS Data Uploader

A tool for importing **school Excel data** (teaching records, attendance, student activities, awards, etc.) into a **MariaDB database** for reporting and analytics.

The uploader’s job is to:
- Clean and normalize messy Excel files (headers, blank columns, inconsistent structures).
- Load data into a **staging table** in MariaDB using fast bulk methods.
- Provide metadata (file hash, batch ID, ingestion time, source year) for tracking.
- Allow downstream tools (Power BI, dashboards, custom apps) to query and visualize the data.

---

## 📂 Project Structure

```
sims-uploader/
├── app/
│   ├── __init__.py             # Makes the ``app`` package importable
│   ├── config.py               # Centralized environment loading helpers
│   └── prep_excel.py           # Excel → CSV preprocessor
│
├── sql/
│   ├── sheet_ingest_config.sql # Configuration table for sheet→staging mappings
│   └── teach_record_raw.sql    # Example staging table DDL
│
├── uploads/                    # Drop source Excel/CSV files here (git-ignored contents)
│   └── .gitkeep
│
├── .env.example                # Template for local environment variables
├── requirements.txt            # Python dependencies
└── README.md                   # Project overview
```

---

## ⚙️ Setup

### Prerequisites
- Ubuntu 22.04+ with Git, Python 3.10+, MariaDB/MySQL
- Virtualenv (`python3 -m venv .venv`)
- Access to create/load into a MariaDB database

### Installation
```bash
git clone git@github.com:usacpcheung/sims-uploader.git
cd sims-uploader
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then edit .env with your database credentials
mkdir -p uploads      # optional: ensure the uploads/ workspace exists
```

The `.env` file is loaded automatically by all ingestion tools via `app.config`, keeping credentials out of source code.

---

## 🚀 Usage

1. **Prepare Database**
   ```sql
   CREATE DATABASE SIMSdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   USE SIMSdata;
   SOURCE sql/sheet_ingest_config.sql;
   SOURCE sql/teach_record_raw.sql;
   ```

2. **Preprocess Excel**
   ```bash
   source .venv/bin/activate
   python -m app.prep_excel uploads/your_file.xlsx
   ```
   Store raw spreadsheets under `uploads/` so related CSVs remain out of Git. The script reads `.env` for database access, then writes a cleaned `.csv` alongside the Excel file. Output filenames are suffixed with the spreadsheet's SHA-256 hash (e.g. `your_file.<hash>.csv`) so re-processing the same worksheet never overwrites previous exports. It exits immediately with a helpful message if any required `DB_...` variables are missing.

   The preprocessor inspects the target staging table schema (via `information_schema`) to determine which headers are mandatory. Columns listed under `required_columns` in the sheet configuration must appear in the spreadsheet, while metadata columns (e.g. `file_hash`) are ignored during validation. When running via the CLI, the tool prints `Missing required column(s): …` to `stderr` and exits with status code `2`. When `app.prep_excel.main` is imported and called from another service (e.g. a future web UI), a `MissingColumnsError` is raised; the exception exposes a `.missing_columns` tuple containing the absent header names so callers can surface a structured error to end users.

   Sheet configuration (sheet→staging-table mapping, metadata columns, and future options) is stored in `sheet_ingest_config`. Each row is scoped by `workbook_type` so different templates can reuse the same worksheet label without clashing. Register the worksheets you plan to ingest with simple SQL instead of editing Python:

   ```sql
   INSERT INTO sheet_ingest_config (
     workbook_type,
     sheet_name,
     staging_table,
     metadata_columns,
     required_columns,
     options
   )
   VALUES (
     'prototype_teaching_records',
     'TEACH_RECORD',
     'teach_record_raw',
     JSON_ARRAY('id', 'file_hash', 'batch_id', 'source_year', 'ingested_at'),
     JSON_ARRAY(
       '記錄狀態', '日期', '任教老師', '學生編號', '姓名', '英文姓名', '性別',
       '學生級別', '病房', '病床', '出勤 (來自出勤記錄輸入)', '出勤', '教學組別',
       '科目', '取代科目', '教授科目', '課程級別', '教材', '課題', '教學重點1',
       '教學重點2', '教學重點3', '教學重點4', '自定課題', '自定教學重點', '練習',
       '上課時數', '備註', '教學跟進/回饋'
     ),
     JSON_OBJECT('rename_last_subject', TRUE)
   )
   ON DUPLICATE KEY UPDATE
     staging_table = VALUES(staging_table),
     metadata_columns = VALUES(metadata_columns),
     required_columns = VALUES(required_columns),
     options = VALUES(options);
   ```

   The `required_columns` JSON array lets you explicitly state which business headers must be present for a given workbook type. Any other non-metadata columns can be treated as optional (for example, year-specific additions). The `options` JSON column toggles sheet-specific behaviours. For example, `rename_last_subject` controls whether unnamed trailing columns are renamed to “教授科目” and other blank unnamed columns are dropped—behaviour that only the prototype teaching-record sheet currently needs. Disable it by setting the flag to `FALSE` when registering other templates.

   To onboard a new Excel layout, create its staging table (e.g. `SOURCE sql/new_sheet_raw.sql;`) and insert the corresponding row into `sheet_ingest_config` with an appropriate `workbook_type`. The preprocessor will automatically pick up the mapping, query the live schema for required headers, and order columns to match the staging table on the next run—no code change required.

   ### Deduplication workflow

   - Each staging table should declare a unique index on `file_hash` (see `sql/teach_record_raw.sql` for an example `UNIQUE KEY`).
   - `app.prep_excel.main` hashes the original workbook before writing CSV output. If the hash already exists in the destination staging table, the script skips CSV generation and logs a warning to `stderr` so automated callers can gracefully short-circuit their pipelines.
   - UI consumers should treat a duplicate submission as a no-op: surface a “file already uploaded” notice to the user, keep the previous ingestion metadata untouched, and avoid queuing a second `LOAD DATA INFILE` job.

   When a new hash is encountered, the CLI prints the generated CSV path (with hash suffix) and the checksum itself on separate lines. Callers can persist both values for auditing and downstream loading.

3. **Load into MariaDB**
   ```sql
   LOAD DATA INFILE '/var/lib/mysql-files/input.csv'
   INTO TABLE teach_record_raw
   FIELDS TERMINATED BY ',' ENCLOSED BY '"'
   LINES TERMINATED BY '\n'
   IGNORE 1 LINES
   SET file_hash = '...', batch_id = UUID(), source_year = 2025, ingested_at = NOW();
   ```

---

## 📊 Roadmap

- Support multiple Excel types (teaching records, attendance, activities, awards).
- Add a FastAPI web interface for uploads and monitoring.
- Normalize staging data into relational tables (e.g. `teachers`, `students`, `subjects`, `activities`).
- Connect with BI tools (Power BI, Superset, custom dashboards).
- Add Docker deployment option.

---

## 🛡️ Notes

- `.xlsx`, `.csv`, `.env`, and everything under `uploads/` are **ignored** by Git — do not commit real student/teacher data.
- Always use `utf8mb4` for safe Unicode (Chinese characters, emoji, etc.).
- Credentials go into a local `.env` file (never pushed to GitHub). When adding new FastAPI apps or CLI commands, import helpers from `app.config` so secrets remain centralized.
