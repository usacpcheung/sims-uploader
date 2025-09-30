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
│   └── prep_excel.py          # Excel → CSV preprocessor
│
├── sql/
│   └── teach_record_raw.sql   # Example staging table DDL
│
├── requirements.txt           # Python dependencies
├── README.md                  # Project overview
└── agents.md                  # AI assistant guide (development notes)
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
```

---

## 🚀 Usage

1. **Prepare Database**
   ```sql
   CREATE DATABASE SIMSdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   USE SIMSdata;
   SOURCE sql/teach_record_raw.sql;
   ```

2. **Preprocess Excel**
   ```bash
   source .venv/bin/activate
   python app/prep_excel.py /path/to/input.xlsx
   ```
   This creates a cleaned `.csv` aligned to the staging table.

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

- `.xlsx`, `.csv`, `.env` files are **ignored** by Git — do not commit real student/teacher data.
- Always use `utf8mb4` for safe Unicode (Chinese characters, emoji, etc.).
- Credentials go into a local `.env` file (never pushed to GitHub).
