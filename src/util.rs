use datalink::id::ID;
use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    ToSql,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SqlID(ID);


impl SqlID {
    #[inline]
    pub fn new_random() -> Self {
        use rand::Rng;
        rand::thread_rng().gen()
    }
}

impl rand::distributions::Distribution<SqlID> for rand::distributions::Standard {
    #[inline]
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> SqlID {
        SqlID(self.sample(rng))
    }
}

impl ToSql for SqlID {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        use rusqlite::types::Value;
        let value = Value::Blob(self.0.as_raw().get().to_be_bytes().into());
        Ok(ToSqlOutput::Owned(value))
    }
}

impl FromSql for SqlID {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        use rusqlite::types::FromSqlError;
        let bytes = value.as_blob()?;
        let blob_size = bytes.len();
        if blob_size != 16 {
            return Err(FromSqlError::InvalidBlobSize {
                expected_size: 16,
                blob_size,
            });
        }
        let mut array = [0; 16];
        array.copy_from_slice(bytes);
        let u128 = u128::from_be_bytes(array);
        if u128 == 0 {
            return Err(FromSqlError::OutOfRange(0));
        }
        // Safety: We just checked that the ID is not 0
        Ok(SqlID(unsafe { ID::new_unchecked(u128) }))
    }
}

impl From<ID> for SqlID {
    #[inline]
    fn from(id: ID) -> Self {
        SqlID(id)
    }
}

impl From<SqlID> for ID {
    #[inline]
    fn from(blob_id: SqlID) -> Self {
        blob_id.0
    }
}
