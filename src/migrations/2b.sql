DROP INDEX IF EXISTS `data_id`;
DROP INDEX IF EXISTS `links_source_id`;
DROP INDEX IF EXISTS `links_key_id`;
DROP INDEX IF EXISTS `links_keyed`;
-- Instructions from https://www.sqlite.org/lang_altertable.html
PRAGMA foreign_keys = off;
CREATE TABLE `values_new` (
    `uuid` BLOB NOT NULL UNIQUE CHECK(length(uuid) = 16),
    `bool` BOOLEAN,
    `u8` UNSIGNED INT(1),
    `i8` INT(1),
    `u16` UNSIGNED INT(2),
    `i16` INT(2),
    `u32` UNSIGNED INT(4),
    `i32` INT(4),
    `u64` UNSIGNED INT(8),
    `i64` INT(8),
    `f32` FLOAT(4),
    `f64` FLOAT(8),
    `str` TEXT,
    PRIMARY KEY (`uuid`)
);
INSERT INTO `values_new`
SELECT `uuid`,
    `bool`,
    `u8`,
    `i8`,
    `u16`,
    `i16`,
    `i32`,
    `u32`,
    `u64`,
    `i64`,
    `f32`,
    `f64`,
    `str`
FROM `values`;
DROP TABLE `values`;
ALTER TABLE `values_new`
    RENAME TO `values`;
CREATE TABLE `links_new` (
    `source_uuid` BLOB NOT NULL CHECK(length(source_uuid) = 16),
    `key_uuid` BLOB CHECK(length(key_uuid) = 16),
    `target_uuid` BLOB NOT NULL CHECK(length(target_uuid) = 16)
);
INSERT INTO `links_new`
SELECT `source_uuid`,
    `key_uuid`,
    `target_uuid`
FROM `links`;
DROP TABLE `links`;
ALTER TABLE `links_new`
    RENAME TO `links`;
CREATE UNIQUE INDEX `data_id` ON `values` (`uuid`);
CREATE INDEX `data_strs` ON `values` (`str`);
CREATE INDEX `links_source` ON `links` (`source_uuid`);
CREATE INDEX `links_key` ON `links` (`key_uuid`);
CREATE INDEX `links_target` ON `links` (`target_uuid`);
CREATE INDEX `links_keyed` ON `links` (`source_uuid`, `key_uuid`);
PRAGMA foreign_key_check;
PRAGMA foreign_keys = on;
PRAGMA user_version = 2;