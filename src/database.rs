use datalink::{
    links::prelude::{Result as LResult, *},
    prelude::*,
    query::Query,
    value::Value,
};
use rusqlite::{params, Connection, Transaction};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    error::{Error, Result},
    query::{build_links, SQLBuilder, SqlFragment},
    storeddata::StoredData,
};

const INIT_DB: &str = include_str!("init_db.sql");
const INSERT_VALUES: &str = "INSERT INTO `values` (id, bool, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64, str)
VALUES (?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)
ON CONFLICT(id)
DO UPDATE
SET bool=excluded.bool, u8=excluded.u8, i8=excluded.i8, u16=excluded.u16, i16=excluded.i16, u32=excluded.u32, i32=excluded.i32, u64=excluded.u64, i64=excluded.i64, f32=excluded.f32, f64=excluded.f64, str=excluded.str;";
const INSERT_LINK_KEYED: &str = "INSERT INTO `links` (source_id, target_id, key_id)
VALUES (?, ?, ?);";
const INSERT_LINK_UNKEYED: &str = "INSERT INTO `links` (source_id, target_id)
VALUES (?, ?);";

#[derive(Clone)]
pub struct Database {
    pub(crate) conn: Arc<Mutex<Connection>>,
}

impl Database {
    #[inline]
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    #[inline]
    pub fn init(&self) -> Result {
        self.conn
            .lock()
            .unwrap()
            .execute_batch(INIT_DB)
            .map_err(Error::from)
    }

    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Connection::open(path).map(Self::new).map_err(Error::from)
    }

    #[inline]
    pub fn open_in_memory() -> Result<Self> {
        Connection::open_in_memory()
            .map(Self::new)
            .map_err(Error::from)
    }

    #[inline]
    pub fn store<D: Unique + ?Sized>(&self, data: &D) -> Result<StoredData> {
        debug_assert!(self.is_initialized());
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        Self::store_inner(&tx, data)?;
        tx.commit()?;
        Ok(StoredData {
            db: self.clone(),
            id: data.id(),
        })
    }

    #[inline]
    fn store_inner<D: Unique + ?Sized>(tx: &Transaction, data: &D) -> Result<()> {
        let mut stmt = tx.prepare_cached(INSERT_VALUES)?;

        let id = data.id();
        let value = Value::from_data(data);

        stmt.execute(params![
            id.to_string(),
            value.as_bool(),
            value.as_u8(),
            value.as_i8(),
            value.as_u16(),
            value.as_i16(),
            value.as_u32(),
            value.as_i32(),
            value.as_u64(),
            value.as_i64(),
            value.as_f32(),
            value.as_f64(),
            value.as_str()
        ])?;

        drop(stmt);

        let mut inserter = Inserter { tx, source_id: id };

        data.provide_links(&mut inserter)?;

        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn get(&self, id: ID) -> StoredData {
        StoredData {
            db: self.clone(),
            id,
        }
    }

    fn is_initialized(&self) -> bool {
        let conn = self.conn.lock().unwrap();

        const VALUES_COL_COUNT: &str = "SELECT COUNT(*) FROM pragma_table_info('values');";
        const LINKS_COL_COUNT: &str = "SELECT COUNT(*) FROM pragma_table_info('links');";

        let values_col_count: u32 = conn
            .query_row(VALUES_COL_COUNT, [], |r| r.get(0))
            .unwrap_or_default();

        if values_col_count != 13 {
            return false;
        }

        let links_col_count: u32 = conn
            .query_row(LINKS_COL_COUNT, [], |r| r.get(0))
            .unwrap_or_default();

        if links_col_count != 3 {
            return false;
        }

        true
    }
}

impl From<Connection> for Database {
    #[inline]
    fn from(conn: Connection) -> Self {
        Self::new(conn)
    }
}

impl Data for Database {
    #[inline]
    fn provide_links(&self, links: &mut dyn Links) -> Result<(), LinkError> {
        let conn = self.conn.lock().unwrap();
        if let Some(path) = conn.path() {
            links.push_link(("path", path.to_owned()))?;
        }

        links.push_link(("last_insert_rowid", conn.last_insert_rowid()))?;
        links.push_link(("last_changes", conn.changes()))?;
        links.push_link(("autocommit", conn.is_autocommit()))?;
        links.push_link(("busy", conn.is_busy()))?;
        drop(conn);

        self.query_links(links, &Default::default())
    }

    fn query_links(&self, links: &mut dyn Links, query: &Query) -> Result<(), LinkError> {
        let context = ("values".into(), "id".into(), "id".into());
        let mut sql = SQLBuilder::new_conjunct(context);
        // Ensure column #0 is the ID
        sql.select("`values`.`id`");
        query.build_sql(&mut sql)?;

        build_links(self, &sql, links, |r| {
            let id = r
                .get_ref(0)?
                .as_str()?
                .parse::<ID>()
                .map_err(|_| Error::InvalidID)?;
            Ok(self.get(id))
        })?;

        Ok(())
    }
}

struct Inserter<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
    source_id: ID,
}

impl Links for Inserter<'_> {
    #[inline]
    fn push_unkeyed(&mut self, target: BoxedData) -> LResult {
        let target = target.into_unique_random();
        Database::store_inner(self.tx, &target)?;
        let target_id = target.id().to_string();

        let mut stmt = self
            .tx
            .prepare_cached(INSERT_LINK_UNKEYED)
            .map_err(LinkError::other)?;
        stmt.execute([self.source_id.to_string(), target_id])
            .map_err(LinkError::other)?;

        CONTINUE
    }

    #[inline]
    fn push_keyed(&mut self, target: BoxedData, key: BoxedData) -> LResult {
        let target = target.into_unique_random();
        Database::store_inner(self.tx, &target)?;
        let target_id = target.id().to_string();

        let key = key.into_unique_random();
        Database::store_inner(self.tx, &key)?;
        let key_id = key.id().to_string();

        let mut stmt = self
            .tx
            .prepare_cached(INSERT_LINK_KEYED)
            .map_err(LinkError::other)?;
        stmt.execute([self.source_id.to_string(), target_id, key_id])
            .map_err(LinkError::other)?;

        CONTINUE
    }

    #[inline]
    fn push(&mut self, target: BoxedData, key: Option<BoxedData>) -> LResult {
        if let Some(key) = key {
            self.push_keyed(target, key)
        } else {
            self.push_unkeyed(target)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalink::data::DataExt;

    fn test_db() -> Database {
        let db = Database::open_in_memory().unwrap();
        db.init().unwrap();
        db
    }

    #[test]
    fn empty() {
        let db = test_db();

        // No data without a key
        let list = db.as_list().unwrap();
        assert_eq!(list.len(), 0);

        let items = db.as_items().unwrap();
        dbg!(items);
    }

    #[test]
    fn in_out() {
        let db = test_db();

        let data = true.into_unique_random();
        let stored = db.store(&data).unwrap();

        assert_eq!(true, stored.as_bool().unwrap());
    }

    #[test]
    fn in_out_vec() {
        let db = test_db();

        let data = vec![1, 2, 3];
        let data = data.into_unique_random();
        let stored = db.store(&data).unwrap();

        let list = stored.as_list().unwrap();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn insert_unique() {
        let db = test_db();

        let data = true.into_unique_random();

        db.store(&data).unwrap();
        let stored = db.store(&data).unwrap();

        assert_eq!(true, stored.as_bool().unwrap());
    }
}
