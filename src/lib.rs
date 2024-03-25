pub mod database;
pub mod error;
#[cfg(feature = "migrations")]
pub mod migration;
mod query;
pub mod storable;
pub mod storeddata;

pub use rusqlite;

#[macro_export]
macro_rules! schema_version {
    () => {
        1i32
    };
}

pub mod prelude {
    pub use crate::database::Database;
    pub use crate::storable::Storable;
    pub use crate::storeddata::StoredData;
}
