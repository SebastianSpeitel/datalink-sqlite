#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid query")]
    InvalidQuery,
    #[error("Invalid ID")]
    InvalidID,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    FromSql(#[from] rusqlite::types::FromSqlError),
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;
