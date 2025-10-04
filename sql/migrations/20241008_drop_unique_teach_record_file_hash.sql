-- Convert teach_record_raw.file_hash unique index to a non-unique index so every
-- row from a workbook can be staged. Run this before applying the updated
-- table definition.
ALTER TABLE `teach_record_raw`
  DROP INDEX `uniq_teach_record_file_hash`,
  ADD INDEX `idx_teach_record_file_hash` (`file_hash`);
