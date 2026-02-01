use std::fmt;
use std::io;

// TODO [M01]: Use this error type throughout the codebase

/// Unified error type for the storage engine.
#[derive(Debug)]
pub enum Error {
    /// IO error from disk operations.
    Io(io::Error),
    /// Data corruption detected (CRC mismatch, bad format, etc).
    Corruption(String),
    /// Key not found.
    NotFound,
    /// Unexpected end of file/data.
    Eof,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::Corruption(msg) => write!(f, "Corruption: {msg}"),
            Error::NotFound => write!(f, "Not found"),
            Error::Eof => write!(f, "Unexpected end of file"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Result type alias used throughout the engine.
pub type Result<T> = std::result::Result<T, Error>;
