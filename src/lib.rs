pub mod database;
pub mod error;
mod query;
pub mod storable;
pub mod storeddata;

pub use rusqlite;

pub mod prelude {
    pub use crate::database::Database;
    pub use crate::storable::Storable;
    pub use crate::storeddata::StoredData;
}
