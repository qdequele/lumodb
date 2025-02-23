use std::os::raw::c_int;
use std::result;

/// Custom result type for LMDB operations
pub type Result<T> = result::Result<T, Error>;

/// LMDB error codes
#[derive(Debug)]
pub enum Error {
    /// Key/data pair already exists
    KeyExists,
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
    /// Environment is already open
    EnvAlreadyOpen,
    /// Environment is not initialized
    EnvNotInitialized,
    /// Environment is read-only
    EnvReadOnly,
    /// Invalid environment configuration
    EnvInvalidConfig,
    /// Environment path is invalid
    EnvInvalidPath,
    /// Environment map size is invalid
    EnvInvalidMapSize,
    /// Environment max readers value is invalid
    EnvInvalidMaxReaders,
    /// Environment flags cannot be modified
    EnvFlagsImmutable,
    /// Environment is closed
    EnvClosed,
    /// Invalid file descriptor
    EnvInvalidFd,
    /// Invalid transaction state
    TxnInvalid,
    /// Transaction is read-only
    TxnReadOnly,
    /// Memory map failed
    MapFailed,
    /// Memory sync failed
    SyncFailed,
    /// Transaction has pending child operations
    TxnHasChild,
    /// Operation not allowed in read-only transaction
    TxnReadOnlyOp,
    /// Meta page update validation failed
    MetaUpdateFailed,
    /// Maximum dirty pages exceeded
    DirtyPagesExceeded,
    /// Operation would exceed maximum number of databases
    MaxDbsExceeded,
    /// Invalid page number or range
    InvalidPageNumber,
    /// Transaction state check failed
    InvalidTxnState,
    /// Parent transaction is invalid
    InvalidParentTxn,
    /// Database operation failed
    DbOperationFailed,
    /// Transaction merge failed
    TxnMergeFailed,
    /// Unknown error code
    Other(c_int),
    /// Database not found in transaction
    DbNotFound,
    /// Page has already been freed
    PageAlreadyFreed,
    /// Cursor not found in transaction
    CursorNotFound,
    /// Cursor is closed
    CursorClosed,
    /// Failed to close cursor
    CursorCloseFailed,
    /// Cursor is invalid
    CursorInvalid,
    /// Failed to free page
    PageFreeFailed,
    /// Invalid state transition
    InvalidStateTransition,
    /// Resource cleanup failed
    ResourceCleanupFailed,
    /// Transaction reached dirty pages limit
    TxnDirtyLimit,
    /// Transaction reached cursor limit
    TxnCursorLimit,
    /// Transaction reached database limit
    TxnDbLimit,
    /// Page checksum validation failed
    PageCorrupted,
    /// Page version mismatch
    PageVersionMismatch,
    /// Page size exceeds environment limit
    PageSizeExceeded,
    /// Error recovery failed
    RecoveryFailed,
    /// Cursor cleanup failed
    CursorCleanupFailed,
    /// Page cleanup failed
    PageCleanupFailed,
    /// Transaction isolation level conflict
    TxnIsolationConflict,
    /// Transaction retry attempts exhausted
    TxnRetryExhausted(Box<Error>),
    /// Transaction deadlock detected
    TxnDeadlock,
    /// Transaction fence conflict
    TxnFenceConflict,
    /// Unknown error occurred
    Unknown,
    /// Invalid parameter provided
    InvalidParam,
    /// Cursor is active
    CursorActive,
    /// Operation not allowed in read-only mode
    ReadOnly,
    /// Environment map failed
    EnvMapFailed,
    /// Permission denied error
    PermissionDenied,
    /// I/O error
    IoError(String),
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::KeyExists => Error::KeyExists,
            Error::NotFound => Error::NotFound,
            Error::PageNotFound => Error::PageNotFound,
            Error::Corrupted => Error::Corrupted,
            Error::Panic => Error::Panic,
            Error::VersionMismatch => Error::VersionMismatch,
            Error::Invalid => Error::Invalid,
            Error::MapFull => Error::MapFull,
            Error::DbsFull => Error::DbsFull,
            Error::ReadersFull => Error::ReadersFull,
            Error::TlsFull => Error::TlsFull,
            Error::TxnFull => Error::TxnFull,
            Error::CursorFull => Error::CursorFull,
            Error::PageFull => Error::PageFull,
            Error::MapResized => Error::MapResized,
            Error::Incompatible => Error::Incompatible,
            Error::BadRslot => Error::BadRslot,
            Error::BadTxn => Error::BadTxn,
            Error::BadValSize => Error::BadValSize,
            Error::BadDbi => Error::BadDbi,
            Error::EnvAlreadyOpen => Error::EnvAlreadyOpen,
            Error::EnvNotInitialized => Error::EnvNotInitialized,
            Error::EnvReadOnly => Error::EnvReadOnly,
            Error::EnvInvalidConfig => Error::EnvInvalidConfig,
            Error::EnvInvalidPath => Error::EnvInvalidPath,
            Error::EnvInvalidMapSize => Error::EnvInvalidMapSize,
            Error::EnvInvalidMaxReaders => Error::EnvInvalidMaxReaders,
            Error::EnvFlagsImmutable => Error::EnvFlagsImmutable,
            Error::EnvClosed => Error::EnvClosed,
            Error::EnvInvalidFd => Error::EnvInvalidFd,
            Error::TxnInvalid => Error::TxnInvalid,
            Error::TxnReadOnly => Error::TxnReadOnly,
            Error::MapFailed => Error::MapFailed,
            Error::SyncFailed => Error::SyncFailed,
            Error::TxnHasChild => Error::TxnHasChild,
            Error::TxnReadOnlyOp => Error::TxnReadOnlyOp,
            Error::MetaUpdateFailed => Error::MetaUpdateFailed,
            Error::DirtyPagesExceeded => Error::DirtyPagesExceeded,
            Error::MaxDbsExceeded => Error::MaxDbsExceeded,
            Error::InvalidPageNumber => Error::InvalidPageNumber,
            Error::InvalidTxnState => Error::InvalidTxnState,
            Error::InvalidParentTxn => Error::InvalidParentTxn,
            Error::DbOperationFailed => Error::DbOperationFailed,
            Error::TxnMergeFailed => Error::TxnMergeFailed,
            Error::Other(code) => Error::Other(*code),
            Error::DbNotFound => Error::DbNotFound,
            Error::PageAlreadyFreed => Error::PageAlreadyFreed,
            Error::CursorNotFound => Error::CursorNotFound,
            Error::CursorClosed => Error::CursorClosed,
            Error::CursorCloseFailed => Error::CursorCloseFailed,
            Error::CursorInvalid => Error::CursorInvalid,
            Error::PageFreeFailed => Error::PageFreeFailed,
            Error::InvalidStateTransition => Error::InvalidStateTransition,
            Error::ResourceCleanupFailed => Error::ResourceCleanupFailed,
            Error::TxnDirtyLimit => Error::TxnDirtyLimit,
            Error::TxnCursorLimit => Error::TxnCursorLimit,
            Error::TxnDbLimit => Error::TxnDbLimit,
            Error::PageCorrupted => Error::PageCorrupted,
            Error::PageVersionMismatch => Error::PageVersionMismatch,
            Error::PageSizeExceeded => Error::PageSizeExceeded,
            Error::RecoveryFailed => Error::RecoveryFailed,
            Error::CursorCleanupFailed => Error::CursorCleanupFailed,
            Error::PageCleanupFailed => Error::PageCleanupFailed,
            Error::TxnIsolationConflict => Error::TxnIsolationConflict,
            Error::TxnRetryExhausted(e) => Error::TxnRetryExhausted(Box::new(*e.clone())),
            Error::TxnDeadlock => Error::TxnDeadlock,
            Error::TxnFenceConflict => Error::TxnFenceConflict,
            Error::Unknown => Error::Unknown,
            Error::InvalidParam => Error::InvalidParam,
            Error::CursorActive => Error::CursorActive,
            Error::ReadOnly => Error::ReadOnly,
            Error::EnvMapFailed => Error::EnvMapFailed,
            Error::PermissionDenied => Error::PermissionDenied,
            Error::IoError(s) => Error::IoError(s.clone()),
        }
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::NotFound, Error::NotFound) => true,
            (Error::KeyExists, Error::KeyExists) => true,
            (Error::PageNotFound, Error::PageNotFound) => true,
            (Error::Corrupted, Error::Corrupted) => true,
            (Error::Panic, Error::Panic) => true,
            (Error::VersionMismatch, Error::VersionMismatch) => true,
            (Error::Invalid, Error::Invalid) => true,
            (Error::MapFull, Error::MapFull) => true,
            (Error::DbsFull, Error::DbsFull) => true,
            (Error::ReadersFull, Error::ReadersFull) => true,
            (Error::TlsFull, Error::TlsFull) => true,
            (Error::TxnFull, Error::TxnFull) => true,
            (Error::CursorFull, Error::CursorFull) => true,
            (Error::PageFull, Error::PageFull) => true,
            (Error::MapResized, Error::MapResized) => true,
            (Error::Incompatible, Error::Incompatible) => true,
            (Error::BadRslot, Error::BadRslot) => true,
            (Error::BadTxn, Error::BadTxn) => true,
            (Error::BadValSize, Error::BadValSize) => true,
            (Error::BadDbi, Error::BadDbi) => true,
            (Error::InvalidParam, Error::InvalidParam) => true,
            (Error::CursorActive, Error::CursorActive) => true,
            (Error::ReadOnly, Error::ReadOnly) => true,
            (Error::EnvMapFailed, Error::EnvMapFailed) => true,
            (Error::DbOperationFailed, Error::DbOperationFailed) => true,
            (Error::Unknown, Error::Unknown) => true,
            (Error::TxnRetryExhausted(e1), Error::TxnRetryExhausted(e2)) => e1 == e2,
            (Error::IoError(s1), Error::IoError(s2)) => s1 == s2,
            _ => false,
        }
    }
}

impl From<c_int> for Error {
    fn from(err: c_int) -> Error {
        match err {
            -30799 => Error::KeyExists,
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
            -30779 => Error::EnvAlreadyOpen,
            -30778 => Error::EnvNotInitialized,
            -30777 => Error::EnvReadOnly,
            -30776 => Error::EnvInvalidConfig,
            -30775 => Error::EnvInvalidPath,
            -30774 => Error::EnvInvalidMapSize,
            -30773 => Error::EnvInvalidMaxReaders,
            -30772 => Error::EnvFlagsImmutable,
            -30771 => Error::EnvClosed,
            -30770 => Error::EnvInvalidFd,
            -30769 => Error::TxnInvalid,
            -30768 => Error::TxnReadOnly,
            -30767 => Error::MapFailed,
            -30766 => Error::SyncFailed,
            -30765 => Error::MaxDbsExceeded,
            -30764 => Error::InvalidPageNumber,
            -30763 => Error::InvalidTxnState,
            -30762 => Error::InvalidParentTxn,
            -30761 => Error::DbOperationFailed,
            -30760 => Error::TxnMergeFailed,
            -30759 => Error::DbNotFound,
            -30758 => Error::PageAlreadyFreed,
            -30757 => Error::CursorNotFound,
            -30756 => Error::CursorClosed,
            -30755 => Error::CursorCloseFailed,
            -30754 => Error::CursorInvalid,
            -30753 => Error::PageFreeFailed,
            -30752 => Error::InvalidStateTransition,
            -30751 => Error::ResourceCleanupFailed,
            -30750 => Error::TxnDirtyLimit,
            -30749 => Error::TxnCursorLimit,
            -30748 => Error::TxnDbLimit,
            -30747 => Error::PageCorrupted,
            -30746 => Error::PageVersionMismatch,
            -30745 => Error::PageSizeExceeded,
            -30744 => Error::RecoveryFailed,
            -30743 => Error::CursorCleanupFailed,
            -30742 => Error::PageCleanupFailed,
            -30741 => Error::TxnIsolationConflict,
            -30740 => Error::TxnRetryExhausted(Box::new(Error::Unknown)),
            -30739 => Error::TxnDeadlock,
            -30738 => Error::TxnFenceConflict,
            -30737 => Error::Unknown,
            -30736 => Error::InvalidParam,
            err => Error::Other(err),
        }
    }
}

// Add structured error conversion
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound,
            std::io::ErrorKind::PermissionDenied => Error::TxnReadOnly,
            std::io::ErrorKind::InvalidData => Error::Corrupted,
            _ => Error::Unknown,
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

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::KeyExists => write!(f, "Key/data pair already exists"),
            Error::NotFound => write!(f, "No matching key/data pair found"),
            Error::PageNotFound => write!(f, "Requested page not found"),
            Error::Corrupted => write!(f, "Database file is corrupted"),
            Error::Panic => write!(f, "Update of meta page failed"),
            Error::VersionMismatch => write!(f, "Database version mismatch"),
            Error::Invalid => write!(f, "File is not a valid LMDB file"),
            Error::MapFull => write!(f, "Environment mapsize limit reached"),
            Error::DbsFull => write!(f, "Environment maxdbs limit reached"),
            Error::ReadersFull => write!(f, "Environment maxreaders limit reached"),
            Error::TlsFull => write!(f, "Thread-local storage keys full"),
            Error::TxnFull => write!(f, "Transaction has too many dirty pages"),
            Error::CursorFull => write!(f, "Too many open cursors"),
            Error::PageFull => write!(f, "Page has not enough space"),
            Error::MapResized => write!(f, "Database contents grew beyond environment mapsize"),
            Error::Incompatible => write!(f, "Operation and DB incompatible"),
            Error::BadRslot => write!(f, "Invalid reuse of reader locktable slot"),
            Error::BadTxn => write!(f, "Transaction must abort, has a child, or is invalid"),
            Error::BadValSize => write!(
                f,
                "Unsupported size of key/DB name/data, or wrong DUPFIXED size"
            ),
            Error::BadDbi => write!(f, "The specified DBI was changed unexpectedly"),
            Error::EnvAlreadyOpen => write!(f, "Environment is already open"),
            Error::EnvNotInitialized => write!(f, "Environment is not initialized"),
            Error::EnvReadOnly => write!(f, "Environment is read-only"),
            Error::EnvInvalidConfig => write!(f, "Invalid environment configuration"),
            Error::EnvInvalidPath => write!(f, "Invalid environment path"),
            Error::EnvInvalidMapSize => write!(f, "Invalid map size"),
            Error::EnvInvalidMaxReaders => write!(f, "Invalid maximum readers value"),
            Error::EnvFlagsImmutable => write!(f, "Environment flags cannot be modified"),
            Error::EnvClosed => write!(f, "Environment is closed"),
            Error::EnvInvalidFd => write!(f, "Invalid file descriptor"),
            Error::TxnInvalid => write!(f, "Invalid transaction state"),
            Error::TxnReadOnly => write!(f, "Transaction is read-only"),
            Error::MapFailed => write!(f, "Memory map operation failed"),
            Error::SyncFailed => write!(f, "Memory sync operation failed"),
            Error::Other(code) => write!(f, "Unknown error code: {}", code),
            Error::TxnHasChild => write!(f, "Transaction has pending child operations"),
            Error::TxnReadOnlyOp => write!(f, "Operation not allowed in read-only transaction"),
            Error::MetaUpdateFailed => write!(f, "Meta page update validation failed"),
            Error::DirtyPagesExceeded => write!(f, "Maximum dirty pages exceeded"),
            Error::MaxDbsExceeded => {
                write!(f, "Operation would exceed maximum number of databases")
            }
            Error::InvalidPageNumber => write!(f, "Invalid page number or range"),
            Error::InvalidTxnState => write!(f, "Transaction state check failed"),
            Error::InvalidParentTxn => write!(f, "Parent transaction is invalid"),
            Error::DbOperationFailed => write!(f, "Database operation failed"),
            Error::TxnMergeFailed => write!(f, "Transaction merge failed"),
            Error::DbNotFound => write!(f, "Database not found in transaction"),
            Error::PageAlreadyFreed => write!(f, "Page has already been freed"),
            Error::CursorNotFound => write!(f, "Cursor not found in transaction"),
            Error::CursorClosed => write!(f, "Cursor is closed"),
            Error::CursorCloseFailed => write!(f, "Failed to close cursor"),
            Error::CursorInvalid => write!(f, "Cursor is invalid"),
            Error::PageFreeFailed => write!(f, "Failed to free page"),
            Error::InvalidStateTransition => write!(f, "Invalid transaction state transition"),
            Error::ResourceCleanupFailed => write!(f, "Failed to cleanup transaction resources"),
            Error::TxnDirtyLimit => write!(f, "Transaction reached dirty pages limit"),
            Error::TxnCursorLimit => write!(f, "Transaction reached cursor limit"),
            Error::TxnDbLimit => write!(f, "Transaction reached database limit"),
            Error::PageCorrupted => write!(f, "Page checksum validation failed"),
            Error::PageVersionMismatch => write!(f, "Page version mismatch"),
            Error::PageSizeExceeded => write!(f, "Page size exceeds environment limit"),
            Error::RecoveryFailed => write!(f, "Error recovery failed"),
            Error::CursorCleanupFailed => write!(f, "Failed to cleanup cursors"),
            Error::PageCleanupFailed => write!(f, "Failed to cleanup pages"),
            Error::TxnIsolationConflict => write!(f, "Transaction isolation level conflict"),
            Error::TxnRetryExhausted(e) => write!(f, "Transaction retry attempts exhausted: {}", e),
            Error::TxnDeadlock => write!(f, "Transaction deadlock detected"),
            Error::TxnFenceConflict => write!(f, "Transaction fence conflict"),
            Error::Unknown => write!(f, "Unknown error occurred"),
            Error::InvalidParam => write!(f, "Invalid parameter provided"),
            Error::CursorActive => write!(f, "Cursor is active"),
            Error::ReadOnly => write!(f, "Operation not allowed in read-only mode"),
            Error::EnvMapFailed => write!(f, "Environment map operation failed"),
            Error::PermissionDenied => write!(f, "Permission denied error"),
            Error::IoError(s) => write!(f, "I/O error: {}", s),
        }
    }
}
