CREATE TABLE IF NOT EXISTS sheet_ingest_config (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    workbook_type VARCHAR(255) NOT NULL DEFAULT 'default',
    sheet_name VARCHAR(255) NOT NULL,
    staging_table VARCHAR(255) NOT NULL,
    metadata_columns JSON NOT NULL,
    options JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_workbook_sheet (workbook_type, sheet_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
