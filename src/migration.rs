use datalink::id::ID;
use rusqlite::{params, Connection};

use crate::database::Database;
use crate::error::{Error, Result};
use crate::util::SqlID;

type Version = i32;

pub struct Migrations<'db> {
    db: &'db Database,
    version: Version,
}

impl<'db> Migrations<'db> {
    #[inline]
    #[must_use]
    pub fn new(db: &'db Database) -> Self {
        let version = db.schema_version().unwrap_or(0);
        Self { db, version }
    }

    #[inline]
    pub fn run_one(&mut self) -> Option<Result<Version>> {
        debug_assert!(self.version >= 0);
        debug_assert!(self.version <= crate::schema_version!());
        debug_assert_eq!(self.version, self.db.schema_version().unwrap_or(0));

        if self.version >= crate::schema_version!() {
            return None;
        }

        macro_rules! migrate_to {
            ($version:literal) => {{
                log::info!(concat!("Migrating to version ", $version, " ..."));
                let mut conn = self.db.conn.lock().unwrap();
                let res = Migration::<$version>::run(&mut conn);
                log::info!(concat!("Migrated to version ", $version));
                res
            }};
        }

        let res = match self.version {
            0 => migrate_to!(1),
            1 => migrate_to!(2),
            v => {
                unreachable!("Unknown version: {v}");
            }
        };

        if let Err(e) = res {
            Some(Err(e))
        } else {
            self.version += 1;
            Some(Ok(self.version))
        }
    }

    #[inline]
    pub fn run_all(self) -> Result<()> {
        for result in self {
            result?;
        }
        Ok(())
    }
}

impl Iterator for Migrations<'_> {
    type Item = Result<Version>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.run_one()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = crate::schema_version!() as usize - self.version as usize;
        (len, Some(len))
    }
}

impl std::iter::ExactSizeIterator for Migrations<'_> {}
impl std::iter::FusedIterator for Migrations<'_> {}

#[inline]
#[must_use]
pub fn migrate(db: &Database) -> Migrations<'_> {
    Migrations::new(db)
}

struct Migration<const V: i32>;

impl Migration<1> {
    fn run(conn: &mut Connection) -> Result<()> {
        conn.execute_batch(include_str!("migrations/1.sql"))?;
        Ok(())
    }
}

impl Migration<2> {
    fn run(conn: &mut Connection) -> Result<()> {
        let tx = conn.transaction()?;
        {
            tx.execute_batch(include_str!("migrations/2a.sql"))?;
            // Convert value rows
            let mut select = tx.prepare("SELECT `id` FROM `values`")?;
            let mut update = tx.prepare("UPDATE `values` SET `uuid` = ? WHERE `id` = ?")?;

            let mut rows = select.query([])?;

            while let Some(row) = rows.next()? {
                let id_str: String = row.get(0)?;
                let id: SqlID = id_str.parse::<ID>().map_err(|_| Error::InvalidID)?.into();
                update.execute(params![id, id_str])?;
            }
            // Convert link rows
            let mut select =
                tx.prepare("SELECT `source_id`, `key_id`, `target_id` FROM `links`")?;
            let mut update = tx.prepare("UPDATE `links` SET `source_uuid` = ?, `key_uuid` = ?, `target_uuid` = ? WHERE `source_id` IS ? AND `key_id` IS ? AND `target_id` IS ?")?;

            let mut rows = select.query([])?;

            while let Some(row) = rows.next()? {
                let source_id_str: String = row.get(0)?;
                let key_id_str: Option<String> = row.get(1)?;
                let target_id_str: String = row.get(2)?;
                let source_id: SqlID = source_id_str
                    .parse::<ID>()
                    .map_err(|_| Error::InvalidID)?
                    .into();
                let key_id: Option<SqlID> = key_id_str
                    .as_ref()
                    .map(|s| s.parse::<ID>().map_err(|_| Error::InvalidID))
                    .transpose()?
                    .map(SqlID::from);
                let target_id: SqlID = target_id_str
                    .parse::<ID>()
                    .map_err(|_| Error::InvalidID)?
                    .into();
                update.execute(params![
                    source_id,
                    key_id,
                    target_id,
                    source_id_str,
                    key_id_str,
                    target_id_str
                ])?;
            }
            tx.execute_batch(include_str!("migrations/2b.sql"))?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datalink::{data::DataExt, Data};

    use super::*;
    use crate::database::Database;

    #[test]
    fn all() {
        let db = Database::open_in_memory().unwrap();
        let migrations = migrate(&db);
        migrations.run_all().unwrap();

        assert_eq!(db.schema_version().unwrap(), crate::schema_version!());
    }

    #[test]
    fn step_by_step() {
        let db = Database::open_in_memory().unwrap();
        let mut migrations = migrate(&db);

        for v in 0..crate::schema_version!() {
            let res = migrations.next().unwrap();
            assert_eq!(res.unwrap(), v + 1);
            assert_eq!(db.schema_version().unwrap(), v + 1);
        }

        assert!(migrations.next().is_none());

        assert_eq!(db.schema_version().unwrap(), crate::schema_version!());
    }

    #[test]
    fn no_data_loss() {
        let db = Database::open_in_memory().unwrap();
        let mut migrations = migrate(&db);

        // Migrate only to version 1
        let v = migrations.next().unwrap().unwrap();
        assert_eq!(v, 1);

        let conn = db.conn.lock().unwrap();

        const INSERTS: &str = r"
            INSERT INTO `values` (`id`) VALUES ('1');
            INSERT INTO `values` (`id`, `bool`) VALUES ('2', 1);
            INSERT INTO `values` (`id`, `str`) VALUES ('3', 'key');
            INSERT INTO `links` (`source_id`, `key_id`, `target_id`) VALUES ('1', '3', '2');
            -- Same Link but without key
            INSERT INTO `links` (`source_id`, `target_id`) VALUES ('1', '2');
        ";
        conn.execute_batch(INSERTS).unwrap();

        drop(conn);

        // Migrate to current version
        migrations.run_all().unwrap();

        let data = db.get("1".parse().unwrap());
        let items = data.as_items().unwrap();
        let list = data.as_list().unwrap();

        assert_eq!(data.get_id(), Some("1".parse().unwrap()));
        assert_eq!(items.len(), 1);

        assert_eq!(items[0].0.get_id(), Some("3".parse().unwrap()));
        assert_eq!(items[0].0.as_str().unwrap(), "key");
        assert_eq!(items[0].1.get_id(), Some("2".parse().unwrap()));
        assert_eq!(items[0].1.as_bool().unwrap(), true);

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].get_id(), Some("2".parse().unwrap()));
        assert_eq!(list[0].as_bool().unwrap(), true);
    }
}
