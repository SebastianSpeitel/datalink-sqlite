CREATE TABLE IF NOT EXISTS `values` (
    `id` TEXT NOT NULL,
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
    PRIMARY KEY (`id`)
);
CREATE TABLE IF NOT EXISTS `links` (
    `source_id` TEXT NOT NULL,
    `key_id` TEXT,
    `target_id` TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `data_id` ON `values` (`id`);
CREATE INDEX IF NOT EXISTS `links_source_id` ON `links` (`source_id`);
CREATE INDEX IF NOT EXISTS `links_key_id` ON `links` (`key_id`);
CREATE INDEX IF NOT EXISTS `links_keyed` ON `links` (`source_id`, `key_id`);

PRAGMA user_version = 1;