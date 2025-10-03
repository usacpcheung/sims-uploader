ALTER TABLE teach_record_raw
    ADD COLUMN processed_at DATETIME NULL DEFAULT NULL AFTER ingested_at;

ALTER TABLE teach_record_raw
    ADD KEY idx_teach_record_file_processed (file_hash, processed_at);
