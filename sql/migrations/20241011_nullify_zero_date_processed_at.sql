-- Normalize legacy processed_at values so the pipeline can resume batches.
UPDATE `teach_record_raw`
SET processed_at = NULL
WHERE processed_at = '0000-00-00 00:00:00';
