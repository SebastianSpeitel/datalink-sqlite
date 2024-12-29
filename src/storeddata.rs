use datalink::{
    prelude::*,
    query::{Filter, Query},
    types::TypeSet,
};

use crate::{database::Database, util::SqlID};

#[derive(Debug, Clone)]
pub struct StoredData {
    pub(crate) db: Database,
    pub(crate) id: ID,
}

impl StoredData {
    fn query_primitives(
        &self,
        request: &mut impl Request,
        conn: &rusqlite::Connection,
    ) -> Result<(), rusqlite::Error> {
        use datalink::link::IsPrimitive;
        const FALLBACK_SQL: &str = "SELECT * FROM `values` WHERE `uuid` = ?";

        let filter = request.query_ref().filter();

        debug_assert!(filter.accepted_key_types().contains::<IsPrimitive>());

        let mut cols = [Option::<&'static str>::None; 12];
        let mut col_iter = cols.iter_mut();

        let prims = filter.accepted_target_types();
        if prims.contains::<bool>() {
            col_iter.next().map(|c| c.replace("bool"));
        }
        if prims.contains::<u8>() {
            col_iter.next().map(|c| c.replace("u8"));
        }
        if prims.contains::<i8>() {
            col_iter.next().map(|c| c.replace("i8"));
        }
        if prims.contains::<u16>() {
            col_iter.next().map(|c| c.replace("u16"));
        }
        if prims.contains::<i16>() {
            col_iter.next().map(|c| c.replace("i16"));
        }
        if prims.contains::<u32>() {
            col_iter.next().map(|c| c.replace("u32"));
        }
        if prims.contains::<i32>() {
            col_iter.next().map(|c| c.replace("i32"));
        }
        if prims.contains::<u64>() {
            col_iter.next().map(|c| c.replace("u64"));
        }
        if prims.contains::<i64>() {
            col_iter.next().map(|c| c.replace("i64"));
        }
        if prims.contains::<f32>() {
            col_iter.next().map(|c| c.replace("f32"));
        }
        if prims.contains::<f64>() {
            col_iter.next().map(|c| c.replace("f64"));
        }
        if prims.contains::<&str>() {
            col_iter.next().map(|c| c.replace("str"));
        }
        if prims.contains::<&[u8]>() {
            col_iter.next().map(|c| c.replace("bytes"));
        }
        drop(prims);
        drop(filter);
        drop(col_iter);

        let mut stmt = match cols {
            [None,..] => {
                return Ok(());
            }
            [Some("bool"), None, ..] => {
                conn.prepare_cached("SELECT `bool` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("u8"), None, ..] => {
                conn.prepare_cached("SELECT `u8` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("i8"), None, ..] => {
                conn.prepare_cached("SELECT `i8` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("u16"), None, ..] => {
                conn.prepare_cached("SELECT `u16` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("i16"), None, ..] => {
                conn.prepare_cached("SELECT `i16` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("u32"), None, ..] => {
                conn.prepare_cached("SELECT `u32` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("i32"), None, ..] => {
                conn.prepare_cached("SELECT `i32` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("u64"), None, ..] => {
                conn.prepare_cached("SELECT `u64` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("i64"), None, ..] => {
                conn.prepare_cached("SELECT `i64` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("f32"), None, ..] => {
                conn.prepare_cached("SELECT `f32` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("f64"), None, ..] => {
                conn.prepare_cached("SELECT `f64` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("str"), None, ..] => {
                conn.prepare_cached("SELECT `str` FROM `values` WHERE `uuid` = ?")?
            }
            [Some("bytes"), None, ..] => {
                conn.prepare_cached("SELECT `bytes` FROM `values` WHERE `uuid` = ?")?
            }
            [Some(c0), None, ..] => {
                conn.prepare_cached(&format!("SELECT `{c0}` FROM `values` WHERE `uuid` = ?"))?
            }
            [Some(c0), Some(c1), None, ..] => conn.prepare_cached(&format!(
                "SELECT `{c0}`, `{c1}` FROM `values` WHERE `uuid` = ?"
            ))?,
            [Some(c0), Some(c1), Some(c2), None, ..] => conn.prepare_cached(&format!(
                "SELECT `{c0}`, `{c1}`, `{c2}` FROM `values` WHERE `uuid` = ?"
            ))?,
            [Some(c0), Some(c1), Some(c2), Some(c3), None, ..] => conn.prepare_cached(&format!(
                "SELECT `{c0}`, `{c1}`, `{c2}`, `{c3}` FROM `values` WHERE `uuid` = ?"
            ))?,
            [Some(c0), Some(c1), Some(c2), Some(c3), Some(c4), None, ..] => {
                conn.prepare_cached(&format!(
                    "SELECT `{c0}`, `{c1}`, `{c2}`, `{c3}`, `{c4}` FROM `values` WHERE `uuid` = ?"
                ))?
            }
            [Some(c0), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5), None, ..] => {
                conn.prepare_cached(&format!(
                    "SELECT `{c0}`, `{c1}`, `{c2}`, `{c3}`, `{c4}`, `{c5}` FROM `values` WHERE `uuid` = ?"
                ))?
            }
            [..,Some(..)] => conn.prepare_cached(FALLBACK_SQL)?,
            _ => {
                let mut sql = String::with_capacity(256);
                sql.push_str("SELECT ");
                for col in cols.into_iter().flatten() {
                    sql.push('`');
                    sql.push_str(col);
                    sql.push_str("`, ");
                }
                sql.push_str(" FROM `values` WHERE `uuid` = ?");
                conn.prepare_cached(&sql)?
            },
        };

        log::trace!(
            "Querying primitives for {:?}: {:?}",
            self.id,
            stmt.expanded_sql()
        );

        let mut rows = stmt.query([SqlID::from(self.id)])?;

        let Some(row) = rows.next()? else {
            return Ok(());
        };

        for (i, col) in cols.into_iter().flatten().enumerate() {
            let value = row.get_ref(i)?;
            match col {
                "bool" => request.provide_from(RequestedValue::<bool>::new(value)),
                "u8" => request.provide_from(RequestedValue::<u8>::new(value)),
                "i8" => request.provide_from(RequestedValue::<i8>::new(value)),
                "u16" => request.provide_from(RequestedValue::<u16>::new(value)),
                "i16" => request.provide_from(RequestedValue::<i16>::new(value)),
                "u32" => request.provide_from(RequestedValue::<u32>::new(value)),
                "i32" => request.provide_from(RequestedValue::<i32>::new(value)),
                "u64" => request.provide_from(RequestedValue::<u64>::new(value)),
                "i64" => request.provide_from(RequestedValue::<i64>::new(value)),
                "f32" => request.provide_from(RequestedValue::<f32>::new(value)),
                "f64" => request.provide_from(RequestedValue::<f64>::new(value)),
                "str" => request.provide_from(RequestedValue::<Str>::new(value)),
                "bytes" => request.provide_from(RequestedValue::<Bytes>::new(value)),
                _ => unreachable!(),
            }
        }

        debug_assert!(rows.next().unwrap().is_none());

        Ok(())
    }

    fn query_links(
        &self,
        request: &mut impl Request,
        conn: &rusqlite::Connection,
    ) -> Result<(), rusqlite::Error> {
        const SQL: &str = "SELECT `key_uuid`, `target_uuid` FROM `links` WHERE `source_uuid` = ?";
        let mut stmt = conn.prepare_cached(SQL)?;

        let mut rows = stmt.query([SqlID::from(self.id)])?;

        while let Some(row) = rows.next()? {
            let key_id: Option<SqlID> = row.get(0)?;
            let target_id: SqlID = row.get(1)?;
            if let Some(key_id) = key_id {
                request.provide((self.db.get(key_id), self.db.get(target_id)));
            } else {
                request.provide((self.db.get(target_id),));
            }
        }

        Ok(())
    }
}

impl Data for StoredData {
    #[inline]
    fn query(&self, request: &mut impl Request) {
        use datalink::link::IsPrimitive;

        request.provide_id(self.id);

        let conn = self.db.conn.read().unwrap();

        if request
            .query_ref()
            .filter()
            .accepted_key_types()
            .contains::<IsPrimitive>()
        {
            if let Err(e) = self.query_primitives(request, &conn) {
                log::warn!("Error querying primitives: {e:?}");
            }
        }

        if let Err(e) = self.query_links(request, &conn) {
            log::warn!("Error querying links: {e:?}");
        }
    }

    #[inline]
    fn get_id(&self) -> Option<ID> {
        Some(self.id)
    }
}

impl Unique for StoredData {
    #[inline]
    fn id(&self) -> ID {
        self.id
    }
}

struct Str;
struct Bytes;

struct RequestedValue<'a, T> {
    value: rusqlite::types::ValueRef<'a>,
    _type: core::marker::PhantomData<T>,
}

impl<'a, T> RequestedValue<'a, T> {
    fn new(value: rusqlite::types::ValueRef<'a>) -> Self {
        Self {
            value,
            _type: core::marker::PhantomData,
        }
    }
}

impl<'a, T> Data for RequestedValue<'a, T>
where
    T: rusqlite::types::FromSql + datalink::Link<'a>,
{
    fn query(&self, request: &mut impl Request) {
        self.query_owned(request);
    }

    fn query_owned(self, request: &mut impl Request) {
        debug_assert!(request.requests_value_of::<T>());
        let Ok(val) = T::column_result(self.value) else {
            return;
        };
        request.provide_unchecked(val);
    }
}

impl Data for RequestedValue<'_, Str> {
    fn query(&self, request: &mut impl Request) {
        self.query_owned(request);
    }

    fn query_owned(self, request: &mut impl Request) {
        debug_assert!(request.requests_value_of::<&str>());
        let Ok(Some(val)) = self.value.as_str_or_null() else {
            return;
        };
        request.provide_unchecked(val);
    }
}

impl Data for RequestedValue<'_, Bytes> {
    fn query(&self, request: &mut impl Request) {
        self.query_owned(request);
    }

    fn query_owned(self, request: &mut impl Request) {
        debug_assert!(request.requests_value_of::<&[u8]>());
        let Ok(Some(val)) = self.value.as_blob_or_null() else {
            return;
        };
        request.provide_unchecked(val);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::database::Database;
    use datalink::data::DataExt;

    #[test]
    fn in_out() {
        let db = Database::open_in_memory().unwrap();
        db.migrate().unwrap();
        let data_in = "Hello, World!".into_unique_random();

        let data_out = db.store(&data_in).unwrap();

        let req = None::<String>;
        let q = datalink::Request::query_ref(&req);
        let f = datalink::query::Query::filter(q);

        dbg!(f.into_simple());

        assert_eq!(data_in.as_string(), data_out.as_string());
        assert_eq!(data_in.id(), data_out.id());
        assert_eq!(data_in.get_id(), data_out.get_id());
    }
}
