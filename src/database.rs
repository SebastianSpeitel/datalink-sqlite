use datalink::{
    link_builder::{LinkBuilder, LinkBuilderError as LBE, LinkBuilderExt},
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
    query::{build_link, SQLBuilder},
    storeddata::StoredData,
};

const INIT_DB: &str = include_str!("init_db.sql");
const INSERT_VALUES: &str = "INSERT INTO `values` (id, bool, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64, str)
VALUES (?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)
ON CONFLICT(id)
DO UPDATE
SET bool=excluded.bool, u8=excluded.u8, i8=excluded.i8, u16=excluded.u16, i16=excluded.i16, u32=excluded.u32, i32=excluded.i32, u64=excluded.u64, i64=excluded.i64, f32=excluded.f32, f64=excluded.f64, str=excluded.str;";
const INSERT_LINK: &str = "INSERT INTO `links` (source_id, key_id, target_id)
VALUES (?, ?, ?);";

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
    pub fn init(&mut self) -> Result {
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
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let id = Self::store_inner(&tx, data)?;
        tx.commit()?;
        Ok(StoredData {
            db: self.clone(),
            id,
        })
    }

    #[inline]
    fn store_inner<D: Unique + ?Sized>(tx: &Transaction, data: &D) -> Result<ID> {
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

        let mut inserter = Inserter {
            tx,
            source_id: id,
            key: None,
            target: None,
        };

        data.provide_links(&mut inserter)?;

        Ok(id)
    }

    #[inline]
    #[must_use]
    pub fn get(&self, id: ID) -> StoredData {
        StoredData {
            db: self.clone(),
            id,
        }
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
    fn provide_links(&self, builder: &mut dyn LinkBuilder) -> Result<(), LBE> {
        let conn = self.conn.lock().unwrap();
        if let Some(path) = conn.path() {
            builder.push(("path", path.to_owned()))?;
        }

        builder.push(("last_insert_rowid", conn.last_insert_rowid()))?;
        builder.push(("last_changes", conn.changes()))?;
        builder.push(("autocommit", conn.is_autocommit()))?;
        builder.push(("busy", conn.is_busy()))?;
        drop(conn);

        self.query_links(builder, &Default::default())
    }

    fn query_links(&self, builder: &mut dyn LinkBuilder, query: &Query) -> Result<(), LBE> {
        let conn = self.conn.lock().unwrap();
        let sql = SQLBuilder::try_from(query)?;

        log::trace!("Running query with: {:?}", &sql);

        let mut stmt = sql.prepare_cached(&conn).map_err(Error::from)?;
        let mut rows = stmt.query(sql.params()).map_err(Error::from)?;

        loop {
            match rows.next() {
                Err(e) => return Err(LBE::Other(Box::new(e))),
                Ok(None) => break,
                Ok(Some(row)) => build_link(builder, row, self.clone()),
            }
        }
        builder.end()
    }
}

struct Inserter<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
    source_id: ID,
    key: Option<BoxedData>,
    target: Option<BoxedData>,
}

impl LinkBuilder for Inserter<'_> {
    #[inline]
    fn set_key(&mut self, key: BoxedData) {
        self.key.replace(key);
    }

    #[inline]
    fn set_target(&mut self, target: BoxedData) {
        self.target.replace(target);
    }

    #[inline]
    fn build(&mut self) -> Result<(), LBE> {
        let key_id = self
            .key
            .take()
            .map(|k| {
                let k = k.into_unique_random();
                let id = Database::store_inner(self.tx, &k)?;
                Ok::<_, Error>(id.to_string())
            })
            .transpose()?;

        let target_id = {
            let t = self.target.take().ok_or(LBE::MissingTarget)?;
            let t = t.into_unique_random();
            let id = Database::store_inner(self.tx, &t)?;
            id.to_string()
        };

        let mut stmt = self.tx.prepare_cached(INSERT_LINK).map_err(Error::from)?;

        stmt.execute(params![self.source_id.to_string(), key_id, target_id,])
            .map_err(Error::from)?;

        Ok(())
    }

    #[inline]
    fn end(&mut self) -> Result<(), LBE> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalink::data::DataExt;

    fn test_db() -> Database {
        let mut db = Database::open_in_memory().unwrap();
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
        use datalink::data::unique::AlwaysUnique;
        let db = test_db();

        let data = AlwaysUnique::<bool, _>::new_random(&true);

        db.store(&data).unwrap();
        let stored = db.store(&data).unwrap();

        assert_eq!(true, stored.as_bool().unwrap());
    }
}
