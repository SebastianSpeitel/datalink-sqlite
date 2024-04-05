#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use datalink::{links::prelude::*, prelude::*, query::Query, value::ValueBuiler};

use crate::{
    database::Database,
    query::{build_links, SQLBuilder, SqlFragment},
    util::SqlID,
};

pub struct StoredData {
    pub(crate) db: Database,
    pub(crate) id: ID,
}

impl Data for StoredData {
    #[inline]
    fn provide_value<'d>(&'d self, value: &mut dyn ValueBuiler<'d>) {
        let conn = self.db.conn.lock().unwrap();
        let mut sql = SQLBuilder::<()>::default();
        sql.select("`values`.`bool`, `values`.`u8`, `values`.`i8`, `values`.`u16`, `values`.`i16`, `values`.`u32`, `values`.`i32`, `values`.`u64`, `values`.`i64`, `values`.`f32`, `values`.`f64`, `values`.`str`");
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

        if let Ok(v) = row.get(0) {
            value.bool(v);
        }
        if let Ok(v) = row.get(1) {
            value.u8(v);
        }
        if let Ok(v) = row.get(2) {
            value.i8(v);
        }
        if let Ok(v) = row.get(3) {
            value.u16(v);
        }
        if let Ok(v) = row.get(4) {
            value.i16(v);
        }
        if let Ok(v) = row.get(5) {
            value.u32(v);
        }
        if let Ok(v) = row.get(6) {
            value.i32(v);
        }
        if let Ok(v) = row.get(7) {
            value.u64(v);
        }
        if let Ok(v) = row.get(8) {
            value.i64(v);
        }
        if let Ok(v) = row.get(9) {
            value.f32(v);
        }
        if let Ok(v) = row.get(10) {
            value.f64(v);
        }
        if let Ok(v) = row.get::<_, String>(11) {
            value.str(v.into());
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

        let context = ("links".into(), "key_uuid".into(), "target_uuid".into());
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
