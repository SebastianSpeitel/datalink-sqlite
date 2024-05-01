#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use datalink::{links::prelude::*, prelude::*, query::Query, rr::prelude::*};

use crate::{
    database::Database,
    query::{build_links, QueryContext, SQLBuilder, SqlFragment},
    util::SqlID,
};

pub struct StoredData {
    pub(crate) db: Database,
    pub(crate) id: ID,
}

impl Data for StoredData {
    #[inline]
    fn provide_value(&self, mut request: Request) {
        self.provide_requested(&mut request).debug_assert_provided();
    }

    #[inline]
    fn provide_requested<R: Req>(&self, request: &mut Request<R>) -> impl Provided {
        let conn = self.db.conn.lock().unwrap();
        let mut sql = SQLBuilder::<()>::default();

        if R::requests::<bool>() {
            sql.select("`values`.`bool` as `bool`");
        }
        if R::requests::<u8>() {
            sql.select("`values`.`u8` as `u8`");
        }
        if R::requests::<i8>() {
            sql.select("`values`.`i8` as `i8`");
        }
        if R::requests::<u16>() {
            sql.select("`values`.`u16` as `u16`");
        }
        if R::requests::<i16>() {
            sql.select("`values`.`i16` as `i16`");
        }
        if R::requests::<u32>() {
            sql.select("`values`.`u32` as `u32`");
        }
        if R::requests::<i32>() {
            sql.select("`values`.`i32` as `i32`");
        }
        if R::requests::<u64>() {
            sql.select("`values`.`u64` as `u64`");
        }
        if R::requests::<i64>() {
            sql.select("`values`.`i64` as `i64`");
        }
        if R::requests::<f32>() {
            sql.select("`values`.`f32` as `f32`");
        }
        if R::requests::<f64>() {
            sql.select("`values`.`f64` as `f64`");
        }
        if R::requests::<String>() || R::requests::<&str>() {
            sql.select("`values`.`str` as `str`");
        }

        sql.from("`values`");
        sql.wher("`uuid` = ?");
        sql.with(SqlID::from(self.id));

        log::trace!("Running query: {:?}", &sql);

        let mut stmt = sql.prepare_cached(&conn).unwrap();
        let mut rows = stmt.query(sql.params()).unwrap();

        let row = match rows.next() {
            Ok(Some(r)) => r,
            Err(e) => {
                log::trace!("Failed to get values: {e}");
                return;
            }
            Ok(None) => return,
        };

        if R::requests::<bool>() {
            if let Ok(v) = row.get("bool") {
                request.provide_bool(v);
            }
        }
        if R::requests::<u8>() {
            if let Ok(v) = row.get("u8") {
                request.provide_u8(v);
            }
        }
        if R::requests::<i8>() {
            if let Ok(v) = row.get("i8") {
                request.provide_i8(v);
            }
        }
        if R::requests::<u16>() {
            if let Ok(v) = row.get("u16") {
                request.provide_u16(v);
            }
        }
        if R::requests::<i16>() {
            if let Ok(v) = row.get("i16") {
                request.provide_i16(v);
            }
        }
        if R::requests::<u32>() {
            if let Ok(v) = row.get("u32") {
                request.provide_u32(v);
            }
        }
        if R::requests::<i32>() {
            if let Ok(v) = row.get("i32") {
                request.provide_i32(v);
            }
        }
        if R::requests::<u64>() {
            if let Ok(v) = row.get("u64") {
                request.provide_u64(v);
            }
        }
        if R::requests::<i64>() {
            if let Ok(v) = row.get("i64") {
                request.provide_i64(v);
            }
        }
        if R::requests::<f32>() {
            if let Ok(v) = row.get("f32") {
                request.provide_f32(v);
            }
        }
        if R::requests::<f64>() {
            if let Ok(v) = row.get("f64") {
                request.provide_f64(v);
            }
        }
        if R::requests::<String>() {
            if let Ok(v) = row.get("str") {
                request.provide_str_owned(v);
            }
        } else if R::requests::<&str>() {
            if let Ok(v) = row.get::<_, String>("str") {
                request.provide_str(v.as_str());
            }
        }

        // todo: blob/bytes and u128, i128
    }

    #[inline]
    fn provide_links(&self, links: &mut dyn Links) -> Result<(), LinkError> {
        self.query_links(links, &Default::default())
    }

    #[inline]
    fn query_links(&self, links: &mut dyn Links, query: &Query) -> Result<(), LinkError> {
        // TODO: when Links provide a way to tell if they need key, target or both
        // we can optimize this query to only select and convert the needed columns to StoredData

        let context = QueryContext {
            table: "links".into(),
            key_col: "key_uuid".into(),
            target_col: "target_uuid".into(),
        };
        let mut sql = SQLBuilder::new_conjunct(context);
        // Ensure column #0 and #1 are the key and target IDs
        sql.select("`links`.`key_uuid`"); // Column #0
        sql.select("`links`.`target_uuid`"); // Column #1
        sql.wher("`links`.`source_uuid` == ?");
        sql.with(SqlID::from(self.id));
        query.build_sql(&mut sql)?;

        build_links(&self.db, &sql, links, |r| {
            let target_id = r.get::<_, SqlID>(1)?;
            let target = self.db.get(target_id.into());

            match r.get::<_, Option<SqlID>>(0)? {
                Some(key_id) => {
                    let key = self.db.get(key_id.into());
                    Ok(MaybeKeyed::Keyed(key, target))
                }
                None => Ok(MaybeKeyed::Unkeyed(target)),
            }
        })?;

        Ok(())
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

        assert_eq!(data_in.as_str(), data_out.as_str());
        assert_eq!(data_in.id(), data_out.id());
        assert_eq!(data_in.get_id(), data_out.get_id());
    }
}
