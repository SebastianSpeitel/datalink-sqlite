use datalink::{prelude::*, Request};

use crate::{database::Database, util::SqlID};

#[derive(Debug, Clone)]
pub struct StoredData {
    pub(crate) db: Database,
    pub(crate) id: ID,
}

impl Data for StoredData {
    #[inline]
    fn query(&self, request: &mut impl Request) {
        request.provide_id(self.id);

        let conn = self.db.conn.read().unwrap();

        let mut stmt = conn
            .prepare_cached("SELECT * FROM `values` WHERE `uuid` = ?")
            .unwrap();

        let mut rows = stmt.query([SqlID::from(self.id)]).unwrap();

        if let Ok(Some(row)) = rows.next() {
            row.get("bool").map(|b: bool| request.provide(b));
            row.get("u8").map(|b: u8| request.provide(b));
            row.get("i8").map(|b: i8| request.provide(b));
            row.get("u16").map(|b: u16| request.provide(b));
            row.get("i16").map(|b: i16| request.provide(b));
            row.get("u32").map(|b: u32| request.provide(b));
            row.get("i32").map(|b: i32| request.provide(b));
            row.get("u64").map(|b: u64| request.provide(b));
            row.get("i64").map(|b: i64| request.provide(b));
            row.get("f32").map(|b: f32| request.provide(b));
            row.get("f64").map(|b: f64| request.provide(b));
            row.get_ref("str")
                .ok()
                .and_then(|v| v.as_str_or_null().ok().flatten())
                .map(|b| request.provide(b));
            row.get_ref("bytes")
                .ok()
                .and_then(|v| v.as_blob_or_null().ok().flatten())
                .map(|b| request.provide(b));
        }
        drop(rows);
        drop(stmt);

        let mut stmt = conn
            .prepare_cached("SELECT * FROM `links` WHERE `source_uuid` = ?")
            .unwrap();

        let mut rows = stmt.query([SqlID::from(self.id)]).unwrap();

        while let Ok(Some(row)) = rows.next() {
            let key_id: Option<SqlID> = row.get("key_uuid").ok();
            let target_id: SqlID = row.get("target_uuid").unwrap();
            if let Some(key_id) = key_id {
                request.provide((self.db.get(key_id.into()), self.db.get(target_id.into())));
            } else {
                request.provide((self.db.get(target_id.into()),));
            }
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

        assert_eq!(data_in.as_string(), data_out.as_string());
        assert_eq!(data_in.id(), data_out.id());
        assert_eq!(data_in.get_id(), data_out.get_id());
    }
}
