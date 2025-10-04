ALTER TABLE `teach_record_raw`
    MODIFY COLUMN `教學跟進/回饋` TEXT NULL;

ALTER TABLE `teach_record_normalized`
    MODIFY COLUMN `教學跟進/回饋` TEXT NULL;
