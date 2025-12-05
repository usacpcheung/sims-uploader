# New workbook ingestion checklist

The latest Excel template was not present in the repository or `./uploads/`, so the config entry for it is still pending. Once the file is available, capture these details so `sheet_ingest_config` can be populated without guesswork:

1. **Sheet names** – open the workbook and note the exact tab names to ingest.
2. **Headers** – record the cleaned header text for each target sheet in order; use these to set `required_columns`.
3. **Column mappings** – map raw headers to staging column names (stick to `snake_case`).
4. **Staging/normalized tables** – pick table names (e.g., `*_raw` and `*_normalized`) and note any `options.column_types` overrides for long text or date columns.
5. **Metadata** – include standard metadata columns (`id`, `file_hash`, `batch_id`, `source_year`, `ingested_at`, `processed_at`).
6. **Time ranges** – capture any source columns that represent date/time spans plus their formats; store them in `time_range_column`/`time_range_format` alongside an `overlap_target_table` if overlap checks are required.

Add a new `VALUES` block to [`sql/sheet_ingest_config.sql`](../sql/sheet_ingest_config.sql) once the workbook fields are known. The `ingest_planner` CLI can draft this SQL for you and reduce manual transcription errors:

```bash
python app/ingest_planner.py uploads/<workbook>.xlsx \
  --workbook-type <type> \   # required for SQL output
  --emit-sql                 # writes a *_sheet_ingest_config.sql next to the plan
```

- The JSON plan includes cleaned headers, suggested staging/normalized tables, metadata columns, and inferred column types (overridable via `--column-type-override COL:TYPE`).
- The generated SQL mirrors the JSON so you can review, tweak names or types, and then paste into `sheet_ingest_config.sql` or run against the database.
