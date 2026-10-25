use std::fmt;
use std::io;
use std::path::PathBuf;

/// Result type used throughout tradedb.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by tradedb.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An operating-system I/O error.
    Io(io::Error),
    /// The file failed an integrity check: a checksum, a structural
    /// invariant, or a page that is not the version its parent refers to.
    Corrupt {
        /// The page involved, when there is one.
        page: Option<u32>,
        /// What was wrong.
        detail: String,
    },
    /// The file exists but is not a tradedb database.
    NotADatabase(String),
    /// The file is a database in a format this version cannot read.
    UnsupportedFormat(String),
    /// An argument was out of range or inconsistent with the database.
    InvalidArgument(String),
    /// A put would insert a key that already exists. The batch was rolled back.
    DuplicateKey {
        /// Symbol of the offending record.
        symbol: u64,
        /// Time of the offending record.
        time: u64,
        /// Sequence number of the offending record.
        seq: u32,
    },
    /// A value is longer than the database's page size allows.
    ValueTooLarge {
        /// Length of the rejected value.
        len: usize,
        /// Largest accepted length.
        max: usize,
    },
    /// A fixed-width symbol received a value of the wrong length.
    RecordSize {
        /// The symbol.
        symbol: u64,
        /// Its declared record size.
        expected: u32,
        /// The length supplied.
        actual: usize,
    },
    /// The transactional metadata would exceed [`META_CAPACITY`](crate::META_CAPACITY).
    MetaFull {
        /// Encoded size the batch would have produced.
        needed: usize,
        /// Capacity of the metadata area.
        capacity: usize,
    },
    /// The database was opened read-only.
    ReadOnly,
    /// This process already has the database open; share the existing [`Db`](crate::Db)
    /// (it is `Send + Sync`) instead of opening it twice.
    AlreadyOpen(PathBuf),
    /// Another process has the database open in a conflicting mode
    /// (one side asked for exclusive access).
    Locked,
    /// Internal retry signal; never returned from the public API.
    #[doc(hidden)]
    Stale,
}

impl Error {
    pub(crate) fn corrupt(page: impl Into<Option<u32>>, detail: impl Into<String>) -> Self {
        Error::Corrupt {
            page: page.into(),
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Error::InvalidArgument(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corrupt { page: Some(p), detail } => write!(f, "database corrupt at page {p}: {detail}"),
            Error::Corrupt { page: None, detail } => write!(f, "database corrupt: {detail}"),
            Error::NotADatabase(m) => write!(f, "not a tradedb database: {m}"),
            Error::UnsupportedFormat(m) => write!(f, "unsupported database format: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::DuplicateKey { symbol, time, seq } => {
                write!(f, "duplicate key (symbol {symbol}, time {time}, seq {seq})")
            }
            Error::ValueTooLarge { len, max } => {
                write!(f, "value of {len} bytes exceeds the {max}-byte limit")
            }
            Error::RecordSize {
                symbol,
                expected,
                actual,
            } => {
                write!(f, "symbol {symbol} stores {expected}-byte records, got {actual} bytes")
            }
            Error::MetaFull { needed, capacity } => {
                write!(f, "metadata needs {needed} bytes but the area holds {capacity}")
            }
            Error::ReadOnly => f.write_str("database is open read-only"),
            Error::AlreadyOpen(p) => write!(f, "database {} is already open in this process", p.display()),
            Error::Locked => f.write_str("database is locked by another process"),
            Error::Stale => f.write_str("snapshot went stale"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
