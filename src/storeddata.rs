#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use datalink::{
    link_builder::{LinkBuilder, LinkBuilderError as LBE},
    prelude::*,
    query::Query,
    value::ValueBuiler,
};

use crate::{
    database::Database,
    query::{build_link, SQLBuilder},
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
        sql.wher("`id` = ?");
        sql.with(self.id.to_string());

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
    fn provide_links(&self, builder: &mut dyn LinkBuilder) -> Result<(), LBE> {
        self.query_links(builder, &Default::default())
    }

    fn query_links(&self, builder: &mut dyn LinkBuilder, query: &Query) -> Result<(), LBE> {
        let conn = self.db.conn.lock().unwrap();
        let mut sql = SQLBuilder::try_from(query).map_err(|e| LBE::Other(Box::new(e)))?;
        sql.wher("`links`.`source_id` == ?");
        sql.with(self.id.to_string());

        log::trace!("Running query: {:?}", &sql);

        let mut stmt = sql
            .prepare_cached(&conn)
            .map_err(|e| LBE::Other(Box::new(e)))?;
        let mut rows = stmt
            .query(sql.params())
            .map_err(|e| LBE::Other(Box::new(e)))?;

        loop {
            match rows.next() {
                Err(e) => return Err(LBE::Other(Box::new(e))),
                Ok(None) => break,
                Ok(Some(row)) => build_link(builder, row, self.db.clone()),
            }
        }
        builder.end()
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
