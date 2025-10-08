CREATE TABLE `upload_jobs` (
    `job_id` CHAR(36) NOT NULL,
    `original_filename` VARCHAR(255) NOT NULL,
    `workbook_name` VARCHAR(255) NULL,
    `worksheet_name` VARCHAR(255) NULL,
    `file_size` BIGINT UNSIGNED NULL,
    `status` VARCHAR(64) NOT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`job_id`),
    KEY `idx_upload_jobs_status_created_at` (`status`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `upload_job_events` (
    `event_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `job_id` CHAR(36) NOT NULL,
    `status` VARCHAR(64) NOT NULL,
    `message` TEXT NULL,
    `event_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`event_id`),
    KEY `idx_upload_job_events_job_id` (`job_id`),
    KEY `idx_upload_job_events_status_event_at` (`status`, `event_at`),
    CONSTRAINT `fk_upload_job_events_job` FOREIGN KEY (`job_id`) REFERENCES `upload_jobs` (`job_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `upload_job_results` (
    `job_id` CHAR(36) NOT NULL,
    `total_rows` INT UNSIGNED NULL,
    `processed_rows` INT UNSIGNED NULL,
    `successful_rows` INT UNSIGNED NULL,
    `rejected_rows` INT UNSIGNED NULL,
    `normalized_table_name` VARCHAR(255) NULL,
    `rejected_rows_path` VARCHAR(512) NULL,
    `coverage_metadata` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`job_id`),
    KEY `idx_upload_job_results_normalized_table_name` (`normalized_table_name`),
    CONSTRAINT `fk_upload_job_results_job` FOREIGN KEY (`job_id`) REFERENCES `upload_jobs` (`job_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
