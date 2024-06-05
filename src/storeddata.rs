use datalink::{
    links::prelude::*,
    prelude::*,
    query::Query,
    rr::TypeSet,
    value::{Provided, ValueQuery, ValueRequest},
};

use crate::{
    database::Database,
    query::{build_links, QueryContext, SQLBuilder, SqlFragment},
    util::SqlID,
};

#[derive(Debug, Clone)]
pub struct StoredData {
    pub(crate) db: Database,
    pub(crate) id: ID,
}

impl Data for StoredData {
    #[inline]
    fn provide_value(&self, request: &mut ValueRequest) {
        self.provide_requested(request).debug_assert_provided();
    }

    #[inline]
    fn provide_requested<Q: ValueQuery>(&self, request: &mut ValueRequest<Q>) -> impl Provided {
        let mut sql = SQLBuilder::default();
        let selected = select_requested(&mut sql, &request.requesting());

        sql.from("`values`");
        sql.wher("`uuid` = ?");
        sql.with(SqlID::from(self.id));

        let conn = self.db.conn.lock().unwrap();
        log::trace!("Running query: {:?}", &sql);

        let mut stmt = sql.prepare_cached(&conn).unwrap();
        let Ok(mut rows) = stmt.query(sql.params()) else {
            log::error!("Failed to run query: {sql:?}");
            return;
        };

        let row = match rows.next() {
            Ok(Some(r)) => r,
            Err(e) => {
                log::warn!("Failed to get values: {e}");
                return;
            }
            Ok(None) => {
                log::warn!("Data without value row: {}", self.id);
                return;
            }
        };

        provide_selected(row, request, selected);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Unused,
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Str,
}

#[allow(unused_assignments)] // last idx increment
fn select_requested(sql: &mut SQLBuilder, requested: &impl TypeSet) -> [Column; 12] {
    let mut selected: [Column; 12] = [Column::Unused; 12];
    let mut idx = 0;

    macro_rules! select {
        ($sql:literal, $col:ident) => {
            sql.select($sql);
            selected[idx] = Column::$col;
            idx += 1;
        };
    }

    if requested.contains_type::<bool>() {
        select!("`values`.`bool` as `bool`", Bool);
    }
    if requested.contains_type::<u8>() {
        select!("`values`.`u8` as `u8`", U8);
    }
    if requested.contains_type::<i8>() {
        select!("`values`.`i8` as `i8`", I8);
    }
    if requested.contains_type::<u16>() {
        select!("`values`.`u16` as `u16`", U16);
    }
    if requested.contains_type::<i16>() {
        select!("`values`.`i16` as `i16`", I16);
    }
    if requested.contains_type::<u32>() {
        select!("`values`.`u32` as `u32`", U32);
    }
    if requested.contains_type::<i32>() {
        select!("`values`.`i32` as `i32`", I32);
    }
    if requested.contains_type::<u64>() {
        select!("`values`.`u64` as `u64`", U64);
    }
    if requested.contains_type::<i64>() {
        select!("`values`.`i64` as `i64`", I64);
    }
    if requested.contains_type::<f32>() {
        select!("`values`.`f32` as `f32`", F32);
    }
    if requested.contains_type::<f64>() {
        select!("`values`.`f64` as `f64`", F64);
    }
    if requested.contains_type::<&str>() {
        select!("`values`.`str` as `str`", Str);
    }

    selected
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn provide_selected<Q: ValueQuery, const C: usize>(
    row: &rusqlite::Row,
    request: &mut ValueRequest<Q>,
    selected: [Column; C],
) {
    use rusqlite::types::ValueRef as V;
    use Column as C;

    for cell in selected
        .into_iter()
        .take_while(|c| *c != Column::Unused)
        .enumerate()
        .map(|(idx, col)| (col, row.get_ref(idx).unwrap()))
    {
        match cell {
            (C::Bool, V::Integer(0)) => request.provide_bool(false),
            (C::Bool, V::Integer(1)) => request.provide_bool(true),
            (C::U8, V::Integer(i)) => request.provide_u8(i as u8),
            (C::I8, V::Integer(i)) => request.provide_i8(i as i8),
            (C::U16, V::Integer(i)) => request.provide_u16(i as u16),
            (C::I16, V::Integer(i)) => request.provide_i16(i as i16),
            (C::U32, V::Integer(i)) => request.provide_u32(i as u32),
            (C::I32, V::Integer(i)) => request.provide_i32(i as i32),
            (C::U64, V::Integer(i)) => request.provide_u64(i as u64),
            (C::I64, V::Integer(i)) => request.provide_i64(i),
            (C::F32, V::Real(f)) => request.provide_f32(f as f32),
            (C::F32, V::Integer(i)) => request.provide_f32(i as f32),
            (C::F64, V::Real(f)) => request.provide_f64(f),
            (C::F64, V::Integer(i)) => request.provide_f64(i as f64),
            (C::Str, V::Text(s)) => {
                debug_assert!(std::str::from_utf8(s).is_ok());
                request.provide_str(unsafe { std::str::from_utf8_unchecked(s) });
            }
            (c, v) => {
                log::warn!("Unexpected value {v:?} for column {c:?}");
            }
        }
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
