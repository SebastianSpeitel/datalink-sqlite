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

        let filter = request.query_ref().filter();

        if let Ok(v) = filter.accepted_value() {
            if let Some(s) = (&v as &dyn core::any::Any).downcast_ref::<String>() {
                dbg!(s);
            }
            dbg!(&v as &dyn core::any::Any);
        }

        debug_assert!(filter
            .accepted_keys()
            .accepted_types()
            .contains::<IsPrimitive>());

        let mut cols = [Option::<&'static str>::None; 12];
        let mut col_iter = cols.iter_mut();

        if filter.accept_value_of::<bool>() {
            col_iter.next().map(|c| c.replace("bool"));
        }
        if filter.accept_value_of::<u8>() {
            col_iter.next().map(|c| c.replace("u8"));
        }
        if filter.accept_value_of::<i8>() {
            col_iter.next().map(|c| c.replace("i8"));
        }
        if filter.accept_value_of::<u16>() {
            col_iter.next().map(|c| c.replace("u16"));
        }
        if filter.accept_value_of::<i16>() {
            col_iter.next().map(|c| c.replace("i16"));
        }
        if filter.accept_value_of::<u32>() {
            col_iter.next().map(|c| c.replace("u32"));
        }
        if filter.accept_value_of::<i32>() {
            col_iter.next().map(|c| c.replace("i32"));
        }
        if filter.accept_value_of::<u64>() {
            col_iter.next().map(|c| c.replace("u64"));
        }
        if filter.accept_value_of::<i64>() {
            col_iter.next().map(|c| c.replace("i64"));
        }
        if filter.accept_value_of::<f32>() {
            col_iter.next().map(|c| c.replace("f32"));
        }
        if filter.accept_value_of::<f64>() {
            col_iter.next().map(|c| c.replace("f64"));
        }
        if filter.accept_value_of::<&str>() {
            col_iter.next().map(|c| c.replace("str"));
        }
        if filter.accept_value_of::<&[u8]>() {
            col_iter.next().map(|c| c.replace("bytes"));
        }
        drop(filter);
        drop(col_iter);

        // dbg!(&cols);

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
            _ => {
                let mut sql = String::with_capacity(256);
                sql.push_str("SELECT ");
                for col in cols.into_iter().flatten() {
                    sql.push('`');
                    sql.push_str(col);
                    sql.push_str("`, ");
                }
                sql.push_str("1 FROM `values` WHERE `uuid` = ?");
                conn.prepare_cached(&sql)?
            },
        };

        log::trace!(
            "Querying primitives for {:?}: {:?}",
            self.id,
            stmt.expanded_sql().unwrap_or_default()
        );

        let mut rows = stmt.query([SqlID::from(self.id)])?;

        let Some(row) = rows.next()? else {
            return Ok(());
        };

        for (i, col) in cols.into_iter().flatten().enumerate() {
            let value = row.get_ref(i)?;
            match col {
                "bool" => request.provide_from(RequestedLink::<bool>::new(value)),
                "u8" => request.provide_from(RequestedLink::<u8>::new(value)),
                "i8" => request.provide_from(RequestedLink::<i8>::new(value)),
                "u16" => request.provide_from(RequestedLink::<u16>::new(value)),
                "i16" => request.provide_from(RequestedLink::<i16>::new(value)),
                "u32" => request.provide_from(RequestedLink::<u32>::new(value)),
                "i32" => request.provide_from(RequestedLink::<i32>::new(value)),
                "u64" => request.provide_from(RequestedLink::<u64>::new(value)),
                "i64" => request.provide_from(RequestedLink::<i64>::new(value)),
                "f32" => request.provide_from(RequestedLink::<f32>::new(value)),
                "f64" => request.provide_from(RequestedLink::<f64>::new(value)),
                "str" => request.provide_from(RequestedLink::<Str>::new(value)),
                "bytes" => request.provide_from(RequestedLink::<Bytes>::new(value)),
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
            .accepted_keys()
            .accepted_types()
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

#[derive(Debug)]
struct RequestedLink<'a, T> {
    value: rusqlite::types::ValueRef<'a>,
    _type: core::marker::PhantomData<T>,
}

impl<'a, T> RequestedLink<'a, T> {
    fn new(value: rusqlite::types::ValueRef<'a>) -> Self {
        Self {
            value,
            _type: core::marker::PhantomData,
        }
    }
}

impl<'a, T> Data for RequestedLink<'a, T>
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

impl Data for RequestedLink<'_, Str> {
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

impl Data for RequestedLink<'_, Bytes> {
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

        dbg!(f.__into_erased());

        assert_eq!(data_in.as_string(), data_out.as_string());
        assert_eq!(data_in.id(), data_out.id());
        assert_eq!(data_in.get_id(), data_out.get_id());
    }
}
