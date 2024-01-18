use datalink::links::LinkError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid query")]
    InvalidQuery,
    #[error(transparent)]
    DataLink(#[from] LinkError),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

impl From<Error> for LinkError {
    #[inline]
    fn from(value: Error) -> Self {
        match value {
            Error::DataLink(lbe) => lbe,
            e => Self::Other(Box::new(e)),
        }
    }
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;
