use datalink::link_builder::LinkBuilderError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid query")]
    InvalidQuery,
    #[error(transparent)]
    LinkBuilder(#[from] LinkBuilderError),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

impl From<Error> for LinkBuilderError {
    #[inline]
    fn from(value: Error) -> Self {
        match value {
            Error::LinkBuilder(lbe) => lbe,
            e => Self::Other(Box::new(e)),
        }
    }
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;
