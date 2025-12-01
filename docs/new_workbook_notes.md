# New workbook ingestion checklist

The latest Excel template was not present in the repository or `./uploads/`, so the config entry for it is still pending. Once the file is available, capture these details so `sheet_ingest_config` can be populated without guesswork:

1. **Sheet names** – open the workbook and note the exact tab names to ingest.
2. **Headers** – record the cleaned header text for each target sheet in order; use these to set `required_columns`.
3. **Column mappings** – map raw headers to staging column names (stick to `snake_case`).
4. **Staging/normalized tables** – pick table names (e.g., `*_raw` and `*_normalized`) and note any `options.column_types` overrides for long text or date columns.
5. **Metadata** – include standard metadata columns (`id`, `file_hash`, `batch_id`, `source_year`, `ingested_at`, `processed_at`).

Add a new `VALUES` block to [`sql/sheet_ingest_config.sql`](../sql/sheet_ingest_config.sql) once the workbook fields are known.
