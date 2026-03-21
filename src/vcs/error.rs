use std::fmt;

/// Errors produced by VCS operations.
#[derive(Debug)]
pub enum Error {
    /// A database operation failed.
    Db { detail: String },
    /// Serialization failed (encoding for storage).
    Serialization { detail: String },
    /// Deserialization failed (decoding from storage).
    Deserialization { detail: String },
    /// Referenced commit does not exist.
    NoSuchCommit { id: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db { detail } => write!(f, "vcs db error: {detail}"),
            Error::Serialization { detail } => {
                write!(f, "vcs serialization error: {detail}")
            }
            Error::Deserialization { detail } => {
                write!(f, "vcs deserialization error: {detail}")
            }
            Error::NoSuchCommit { id } => {
                write!(f, "no such commit: {id}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<criome_cozo::Error> for Error {
    fn from(err: criome_cozo::Error) -> Self {
        Error::Db {
            detail: err.to_string(),
        }
    }
}
