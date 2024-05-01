use datalink::prelude::*;

use crate::{database::Database, error::Result, storeddata::StoredData};

pub trait Storable {
    fn store(&self, db: &Database) -> Result<StoredData>;

    #[inline]
    fn into_stored(self, db: &Database) -> Result<StoredData>
    where
        Self: Sized,
    {
        self.store(db)
    }
}

impl<D: Data + Unique> Storable for D {
    #[inline]
    fn store(&self, db: &Database) -> Result<StoredData> {
        db.store(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use datalink::data::DataExt;

    #[test]
    fn test_storable() {
        let db = Database::open_in_memory().unwrap();
        db.init().unwrap();
        let data = true.into_unique_random();
        let stored = data.store(&db).unwrap();
        assert_eq!(stored.as_bool().unwrap(), true);
    }
}
