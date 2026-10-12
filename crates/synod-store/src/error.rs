//! Errors.

use std::fmt;
use std::io;

use synod_core::HlcError;

/// Errors from the store.
#[derive(Debug)]
pub enum Error {
    /// SQLite failed.
    Sql(rusqlite::Error),
    /// A file operation failed.
    Io(io::Error),
    /// A schema statement is not valid.
    Schema(String),
    /// A write breaks a local constraint (unique, check, foreign key, not
    /// null, or type).
    Constraint(String),
    /// A call has arguments that are not valid.
    Invalid(String),
    /// The hybrid logical clock failed.
    Clock(HlcError),
    /// An epoch cannot be sealed yet: the local clock has not reached its
    /// end.
    TooEarly {
        /// The epoch.
        epoch: u64,
        /// Local physical clock at which the epoch ends.
        ends_at_ms: u64,
    },
    /// Stored data is not valid.
    Corrupt(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Sql(e) => write!(f, "SQLite: {e}"),
            Error::Io(e) => write!(f, "I/O: {e}"),
            Error::Schema(m) => write!(f, "schema: {m}"),
            Error::Constraint(m) => write!(f, "constraint: {m}"),
            Error::Invalid(m) => write!(f, "invalid: {m}"),
            Error::Clock(e) => write!(f, "clock: {e}"),
            Error::TooEarly { epoch, ends_at_ms } => {
                write!(f, "epoch {epoch} ends at {ends_at_ms} ms; it cannot be sealed yet")
            }
            Error::Corrupt(m) => write!(f, "corrupt data: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Sql(e) => Some(e),
            Error::Io(e) => Some(e),
            Error::Clock(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::Sql(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<HlcError> for Error {
    fn from(e: HlcError) -> Error {
        Error::Clock(e)
    }
}

/// Result type of the store.
pub type Result<T> = std::result::Result<T, Error>;
