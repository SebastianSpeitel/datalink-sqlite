ALTER TABLE `values` ADD COLUMN `uuid` BLOB;

ALTER TABLE `links` ADD COLUMN `source_uuid` BLOB;
ALTER TABLE `links` ADD COLUMN `key_uuid` BLOB;
ALTER TABLE `links` ADD COLUMN `target_uuid` BLOB;
