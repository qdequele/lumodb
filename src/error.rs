use std::os::raw::c_int;
use std::result;

/// Custom result type for LMDB operations
pub type Result<T> = result::Result<T, Error>;

/// LMDB error codes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Error {
    /// Key/data pair already exists
    KeyExist,
    /// No matching key/data pair found
    NotFound,
    /// Requested page not found
    PageNotFound,
    /// Database file is corrupted
    Corrupted,
    /// Update of meta page failed
    Panic,
    /// Database version mismatch
    VersionMismatch,
    /// File is not a valid LMDB file
    Invalid,
    /// Environment mapsize limit reached
    MapFull,
    /// Environment maxdbs limit reached
    DbsFull,
    /// Environment maxreaders limit reached
    ReadersFull,
    /// Thread-local storage keys full
    TlsFull,
    /// Transaction has too many dirty pages
    TxnFull,
    /// Too many open cursors
    CursorFull,
    /// Page has not enough space
    PageFull,
    /// Database contents grew beyond environment mapsize
    MapResized,
    /// Operation and DB incompatible
    Incompatible,
    /// Invalid reuse of reader locktable slot
    BadRslot,
    /// Transaction must abort, has a child, or is invalid
    BadTxn,
    /// Unsupported size of key/DB name/data, or wrong DUPFIXED size
    BadValSize,
    /// The specified DBI was changed unexpectedly
    BadDbi,
    /// Unknown error code
    Other(c_int),
}

impl From<c_int> for Error {
    fn from(err: c_int) -> Error {
        match err {
            -30799 => Error::KeyExist,
            -30798 => Error::NotFound,
            -30797 => Error::PageNotFound,
            -30796 => Error::Corrupted,
            -30795 => Error::Panic,
            -30794 => Error::VersionMismatch,
            -30793 => Error::Invalid,
            -30792 => Error::MapFull,
            -30791 => Error::DbsFull,
            -30790 => Error::ReadersFull,
            -30789 => Error::TlsFull,
            -30788 => Error::TxnFull,
            -30787 => Error::CursorFull,
            -30786 => Error::PageFull,
            -30785 => Error::MapResized,
            -30784 => Error::Incompatible,
            -30783 => Error::BadRslot,
            -30782 => Error::BadTxn,
            -30781 => Error::BadValSize,
            -30780 => Error::BadDbi,
            err => Error::Other(err),
        }
    }
}

/// Helper trait for converting C error codes to Results
trait IntoResult {
    fn into_result(self) -> Result<()>;
}

impl IntoResult for c_int {
    fn into_result(self) -> Result<()> {
        if self == 0 {
            Ok(())
        } else {
            Err(Error::from(self))
        }
    }
}
