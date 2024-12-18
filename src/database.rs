use datalink::{prelude::*, Query, Receiver};
use rusqlite::{params, Connection, Transaction};
use std::{
    cell::OnceCell,
    path::Path,
    sync::{Arc, RwLock},
};

use crate::{error::Result, storeddata::StoredData, util::SqlID};

const INSERT_LINK_KEYED: &str = "INSERT INTO `links` (source_uuid, target_uuid, key_uuid)
VALUES (?, ?, ?);";
const INSERT_LINK_UNKEYED: &str = "INSERT INTO `links` (source_uuid, target_uuid)
VALUES (?, ?);";

#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) conn: Arc<RwLock<Connection>>,
}

impl Database {
    #[inline]
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(RwLock::new(conn)),
        }
    }

    #[inline]
    pub fn init(&self) -> Result {
        log::info!("Initializing");
        if self.is_ready() {
            log::info!("Already initialized");
            return Ok(());
        }

        let mut conn = self.conn.write().unwrap();
        let tx = conn.transaction()?;

        tx.execute_batch(include_str!("migrations/1.sql"))?;
        tx.execute_batch(include_str!("migrations/2a.sql"))?;
        tx.execute_batch(include_str!("migrations/2b.sql"))?;

        tx.commit()?;
        drop(conn);
        debug_assert!(self.is_ready());
        log::debug!("Initialized");
        Ok(())
    }

    #[cfg(feature = "migrations")]
    #[inline]
    pub fn migrate(&self) -> Result {
        log::info!("Migrating");
        crate::migration::Migrations::new(self).run_all()
    }

    #[inline]
    pub fn schema_version(&self) -> Result<i32> {
        const SQL: &str = "SELECT user_version FROM pragma_user_version();";

        let conn = self.conn.read().unwrap();
        let version = conn.query_row(SQL, [], |r| r.get(0))?;
        Ok(version)
    }

    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Connection::open(path).map(Self::new).map_err(From::from)
    }

    #[inline]
    pub fn open_in_memory() -> Result<Self> {
        Connection::open_in_memory()
            .map(Self::new)
            .map_err(From::from)
    }

    #[inline]
    pub fn store<D: Data + Unique>(&self, data: &D) -> Result<StoredData> {
        debug_assert!(self.is_ready());
        let mut conn = self.conn.write().unwrap();
        let tx = conn.transaction()?;

        let mut upserter = Upserter::new(&tx);
        upserter.id.set(data.id().into()).unwrap();
        data.query(&mut upserter);
        drop(upserter);

        tx.commit()?;
        Ok(StoredData {
            db: self.clone(),
            id: data.id(),
        })
    }

    #[inline]
    #[must_use]
    pub fn get(&self, id: impl Into<ID>) -> StoredData {
        StoredData {
            db: self.clone(),
            id: id.into(),
        }
    }

    #[inline]
    fn is_ready(&self) -> bool {
        self.schema_version()
            .is_ok_and(|v| v == crate::schema_version!())
        // const VALUES_COL_COUNT: &str = "SELECT COUNT(*) FROM pragma_table_info('values');";
        // const LINKS_COL_COUNT: &str = "SELECT COUNT(*) FROM pragma_table_info('links');";
        // const SCHEMA_VERSION: &str = "SELECT user_version FROM pragma_user_version();";

        // let conn = self.conn.lock().unwrap();

        // let schema_version: i32 = conn
        //     .query_row(SCHEMA_VERSION, [], |r| r.get(0))
        //     .unwrap_or_default();

        // if schema_version != crate::schema_version!() {
        //     return false;
        // }

        // let values_col_count: u32 = conn
        //     .query_row(VALUES_COL_COUNT, [], |r| r.get(0))
        //     .unwrap_or_default();

        // if values_col_count != 13 {
        //     return false;
        // }
        // let links_col_count: u32 = conn
        //     .query_row(LINKS_COL_COUNT, [], |r| r.get(0))
        //     .unwrap_or_default();

        // if links_col_count != 3 {
        //     return false;
        // }

        // true
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
    fn query(&self, request: &mut impl datalink::Request) {
        let conn = self.conn.read().unwrap();
        if let Some(p) = conn.path() {
            request.provide(("path", p.to_owned()));
        }

        request.provide(("last_insert_rowid", conn.last_insert_rowid()));
        request.provide(("last_changes", conn.changes()));
        request.provide(("autocommit", conn.is_autocommit()));
        request.provide(("busy", conn.is_busy()));

        const SQL: &str = "SELECT `values`.`uuid` AS `uuid` FROM `values`
        UNION
        SELECT `links`.`source_uuid` AS `uuid` FROM `links`; ";
        let mut stmt = conn.prepare_cached(SQL).unwrap();

        let mut rows = stmt.query([]).unwrap();

        while let Ok(Some(row)) = rows.next() {
            let id: SqlID = row.get(0).unwrap();
            let data = self.get(id);
            request.provide((data,));
        }
    }
}

trait AsID: core::fmt::Debug {
    fn as_id(&self) -> SqlID;
}

impl AsID for ID {
    #[inline]
    fn as_id(&self) -> SqlID {
        SqlID::from(*self)
    }
}

impl AsID for SqlID {
    #[inline]
    fn as_id(&self) -> SqlID {
        *self
    }
}

impl AsID for OnceCell<SqlID> {
    #[inline]
    fn as_id(&self) -> SqlID {
        *self.get_or_init(SqlID::new_random)
    }
}

impl AsID for &OnceCell<SqlID> {
    #[inline]
    fn as_id(&self) -> SqlID {
        *self.get_or_init(SqlID::new_random)
    }
}

#[derive(Debug)]
struct Upserter<'tx, ID: AsID> {
    tx: &'tx Transaction<'tx>,
    id: ID,
    next_key_id: OnceCell<SqlID>,
    next_target_id: OnceCell<SqlID>,
}

impl<'tx> Upserter<'tx, OnceCell<SqlID>> {
    fn new(tx: &'tx Transaction) -> Self {
        Self {
            tx,
            id: OnceCell::new(),
            next_key_id: OnceCell::new(),
            next_target_id: OnceCell::new(),
        }
    }
}

impl<'tx, ID: AsID> Upserter<'tx, ID> {
    fn new_key(&self) -> Upserter<'tx, &'_ OnceCell<SqlID>> {
        Upserter {
            tx: self.tx,
            id: &self.next_key_id,
            next_key_id: OnceCell::new(),
            next_target_id: OnceCell::new(),
        }
    }

    fn new_target(&self) -> Upserter<'tx, &'_ OnceCell<SqlID>> {
        Upserter {
            tx: self.tx,
            id: &self.next_target_id,
            next_key_id: OnceCell::new(),
            next_target_id: OnceCell::new(),
        }
    }

    fn finish_link(&mut self) -> Result<(), rusqlite::Error> {
        let key_id = self.next_key_id.take();
        let Some(target_id) = self.next_target_id.take() else {
            return Ok(());
        };

        if let Some(key_id) = key_id {
            let mut stmt = self.tx.prepare_cached(INSERT_LINK_KEYED)?;
            stmt.execute([self.id.as_id(), target_id, key_id])?;
        } else {
            let mut stmt = self.tx.prepare_cached(INSERT_LINK_UNKEYED)?;
            stmt.execute([self.id.as_id(), target_id])?;
        }

        Ok(())
    }
}

impl<'tx, ID: AsID> Drop for Upserter<'tx, ID> {
    fn drop(&mut self) {
        let _ = self.finish_link();
    }
}

impl<'tx, ID: AsID> Query for Upserter<'tx, ID> {
    type Filter<'q>
        = datalink::query::AcceptAny
    where
        Self: 'q;
    type KeyQuery<'q>
        = Upserter<'tx, &'q OnceCell<SqlID>>
    where
        Self: 'q;
    type TargetQuery<'q>
        = Upserter<'tx, &'q OnceCell<SqlID>>
    where
        Self: 'q;
    type Receiver<'q>
        = &'q mut Self
    where
        Self: 'q;

    fn filter(&self) -> Self::Filter<'_> {
        Default::default()
    }

    fn link_query(&mut self) -> (Self::TargetQuery<'_>, Self::KeyQuery<'_>) {
        self.finish_link().unwrap();

        (self.new_target(), self.new_key())
    }

    fn key_query(&mut self) -> Self::KeyQuery<'_> {
        self.new_key()
    }

    fn target_query(&mut self) -> Self::TargetQuery<'_> {
        self.new_target()
    }

    fn receiver(&mut self) -> Self::Receiver<'_> {
        self
    }
}

macro_rules! insert_impl {
    ($f:ident($ty:ty) => $col:literal) => {
        fn $f(&mut self, value: $ty) {
            const SQL: &str = concat!(
                "INSERT INTO `values` (uuid, ",
                $col,
                ") VALUES (?, ?) ON CONFLICT(uuid) DO UPDATE SET ",
                $col,
                "=excluded.",
                $col,
                ";"
            );
            let mut stmt = self.tx.prepare_cached(SQL).unwrap();
            stmt.execute(params![self.id.as_id(), value]).unwrap();
        }
    };
}

#[warn(clippy::missing_trait_methods)]
impl<ID: AsID> Receiver for Upserter<'_, ID> {
    insert_impl!(bool(bool) => "bool");
    insert_impl!(u8(u8) => "u8");
    insert_impl!(i8(i8) => "i8");
    insert_impl!(u16(u16) => "u16");
    insert_impl!(i16(i16) => "i16");
    insert_impl!(u32(u32) => "u32");
    insert_impl!(i32(i32) => "i32");
    insert_impl!(u64(u64) => "u64");
    insert_impl!(i64(i64) => "i64");
    // insert_impl!(u128(u128) => "u128");
    // insert_impl!(i128(i128) => "i128");
    insert_impl!(f32(f32) => "f32");
    insert_impl!(f64(f64) => "f64");
    insert_impl!(str(&str) => "str");

    #[inline]
    fn other_ref(&mut self, value: &dyn std::any::Any) {
        if let Some(id) = value.downcast_ref::<datalink::id::ID>() {
            assert_eq!(self.id.as_id(), id.as_id());
            return;
        }

        if let Some(id) = value.downcast_ref::<SqlID>() {
            assert_eq!(self.id.as_id(), *id);
            return;
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
        let list = db.as_list();
        assert!(list.len() > 0);

        let items = db.as_items();
        dbg!(items);
    }

    #[test]
    fn in_out_bool() {
        let db = test_db();

        let data = true.into_unique_random();
        let stored = db.store(&data).unwrap();

        dbg!(&db as &ErasedData);

        assert_eq!(true, stored.as_bool().unwrap());
    }

    #[test]
    fn in_out_vec() {
        let db = test_db();

        let data = vec![1, 2, 3];
        let data = data.into_unique_random();
        let stored = db.store(&data).unwrap();

        let list = stored.as_list();

        dbg!(&db as &ErasedData);
        dbg!(&list);

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

    #[test]
    #[should_panic]
    fn uninitialized() {
        let db = Database::open_in_memory().unwrap();
        db.store(&true.into_unique_random()).unwrap();
    }
}
