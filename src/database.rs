use std::ffi::CString;
use std::ptr::null_mut;
use std::cell::RefCell;
use std::any::Any;
use std::cmp::Ordering;
use std::ptr::NonNull;

use crate::cursor::Cursor;
use crate::transaction::Transaction;
use crate::error::{Error, Result};
use crate::value::Value;
use crate::constants::{
    // Flags
    DbFlags, WriteFlags,
    // Global constants
    CORE_DBS,

};

/// Database statistics
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    /// Database depth
    pub(crate) depth: u16,
    /// Number of branch pages
    pub(crate) branch_pages: usize,
    /// Number of leaf pages 
    pub(crate) leaf_pages: usize,
    /// Number of overflow pages
    pub(crate) overflow_pages: usize,
    /// Number of entries
    pub(crate) entries: usize,
}

/// Database metadata
pub(crate) struct DatabaseInfo {
    /// The name of the database. None for the default/unnamed database.
    /// Must be a valid UTF-8 string if provided.
    name: Option<CString>,
    
    /// Unique identifier for this database within the environment.
    /// Values 0 and 1 are reserved for internal LMDB use.
    dbi: u32,
    
    /// Configuration flags for this database.
    /// See constants.rs for valid flag values (REVERSEKEY, DUPSORT, etc).
    flags: u32,
    
    /// Optional custom comparison function for sorting keys.
    /// If None, uses lexicographical byte ordering.
    /// Must be consistent across all operations on this database.
    cmp: Option<Box<dyn Fn(&[u8], &[u8]) -> Ordering>>,
    
    /// Optional custom comparison function for sorting duplicate values.
    /// Only used if DUPSORT flag is set.
    /// If None, uses lexicographical byte ordering.
    dupsort: Option<Box<dyn Fn(&[u8], &[u8]) -> Ordering>>,
    
    /// User relocate function for custom page management.
    /// Currently not implemented.
    rel: Option<Box<dyn Fn()>>,
    
    /// User-provided context for the relocate function.
    /// Currently not implemented.
    relctx: Option<Box<dyn Any>>,
}

impl Default for DatabaseInfo {
    fn default() -> Self {
        Self {
            name: None,
            dbi: 0,
            flags: 0,
            cmp: None,
            dupsort: None,
            rel: None,
            relctx: None,
        }
    }
}

/// Database handle for LMDB database operations.
/// 
/// This handle provides access to a single database within an LMDB environment.
/// Multiple databases can exist within the same environment if the environment
/// was configured with `MDB_MAXDBS`.
///
/// # Thread Safety
/// 
/// The database handle itself is not thread-safe and should not be shared between
/// threads without external synchronization. However, multiple threads can safely
/// access the same database through different transactions.
///
/// # Examples
/// 
/// ```
/// use lmdb_rs::{Environment, Database, Transaction};
/// 
/// let env = Environment::new().unwrap();
/// let txn = Transaction::begin(&env, None).unwrap();
/// let db = Database::open(&txn, Some("mydb"), 0).unwrap();
/// ```
pub struct Database {
    /// Database identifier
    dbi: u32,
    /// Database flags 
    flags: DbFlags,
    /// Reference to owning transaction
    txn: NonNull<Transaction>,
    /// Database statistics
    stats: RefCell<DbStats>,
    /// Database metadata
    info: RefCell<DatabaseInfo>,
}

impl Database {
    /// Opens a database in the environment.
    ///
    /// # Arguments
    /// * `txn` - The transaction to use for opening the database
    /// * `name` - Optional database name. If None, opens the default unnamed database
    /// * `flags` - Database configuration flags
    ///
    /// # Errors
    /// * `Error::DbsFull` - Maximum number of databases has been reached
    /// * `Error::Invalid` - Invalid database name or flags
    /// * `Error::BadRslot` - Bad reuse of reader slot
    /// * Other LMDB-specific errors
    ///
    /// # Thread Safety
    /// This operation is not thread-safe and should only be called when no other
    /// threads are accessing the database.
    pub fn open(txn: &Transaction, name: Option<&str>, flags: DbFlags) -> Result<Self> {
        if !txn.is_valid() {
            return Err(Error::InvalidTxnState);
        }

        if flags.contains(DbFlags::DUPSORT) && flags.contains(DbFlags::REVERSEKEY) {
            return Err(Error::Incompatible);
        }

        let name_cstr = match name {
            Some(n) => Some(CString::new(n).map_err(|_| Error::EnvInvalidConfig)?),
            None => None,
        };
        
        let dbi = txn.allocate_dbi().map_err(|e| match e {
            Error::DbsFull => Error::MaxDbsExceeded,
            Error::BadRslot => Error::InvalidStateTransition,
            e => e
        })?;
        
        let info = DatabaseInfo {
            name: name_cstr,
            dbi,
            flags: flags.bits(),
            cmp: None,
            dupsort: None,
            rel: None,
            relctx: None,
        };

        let db = Database {
            dbi,
            flags,
            txn: NonNull::new(txn).ok_or(Error::BadTxn)?,
            stats: RefCell::new(DbStats::default()),
            info: RefCell::new(info),
        };

        txn.register_database(&db, db.info.borrow().name.as_ref())
            .map_err(|e| match e {
                Error::Invalid => Error::DbOperationFailed,
                Error::BadTxn => Error::InvalidTxnState,
                Error::DbsFull => Error::MaxDbsExceeded,
                e => e
            })?;

        Ok(db)
    }

    /// Closes the database handle.
    ///
    /// # Errors
    /// * `Error::BadTxn` - Transaction has active cursors
    /// * `Error::Invalid` - Database handle is already closed
    ///
    /// # Safety
    /// This operation is not thread-safe. The database should not be closed while
    /// other threads might be accessing it. All cursors must be closed before
    /// calling this method.
    pub fn close(&mut self) -> Result<()> {
        self.validate_db_state()?;

        if !self.txn.as_ref().is_valid() {
            return Err(Error::InvalidTxnState);
        }

        if self.txn.as_ref().has_active_cursors(self.dbi) {
            return Err(Error::ResourceCleanupFailed);
        }

        if let Err(_) = self.info.try_borrow_mut() {
            return Err(Error::ResourceCleanupFailed);
        }
        *self.info.get_mut() = DatabaseInfo::default();

        if let Err(_) = self.stats.try_borrow_mut() {
            return Err(Error::ResourceCleanupFailed);
        }
        *self.stats.get_mut() = DbStats::default();

        self.dbi = 0;
        self.flags = DbFlags::empty();
        
        Ok(())
    }

    /// Gets database statistics.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for reading statistics
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid
    ///
    /// # Thread Safety
    /// Safe to call from multiple threads using different transactions.
    pub fn stat(&self, txn: &Transaction) -> Result<&DbStats> {
        self.validate_transaction(txn)?;

        let stats = txn.get_db_stats(self.dbi)
            .map_err(|e| match e {
                Error::BadTxn => Error::InvalidTxnState,
                Error::BadDbi => Error::BadDbi,
                e => Error::DbOperationFailed
            })?;

        *self.stats.try_borrow_mut()
            .map_err(|_| Error::ResourceCleanupFailed)? = stats;
        
        Ok(&self.stats.borrow())
    }

    /// Get database flags.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for reading flags
    ///
    /// # Returns
    /// The database configuration flags as set during database creation.
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid
    pub fn flags(&self, txn: &Transaction) -> Result<u32> {
        self.validate_transaction(txn)?;
        Ok(self.flags.bits())
    }

    /// Drop a database.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for dropping the database
    /// * `del` - If true, delete the database from the environment.
    ///          If false, just empty the database but keep it in the environment.
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid or read-only
    /// * `Error::EACCES` - Write permission denied
    ///
    /// # Safety
    /// This operation requires an exclusive write transaction.
    /// The database must not be accessed after being dropped.
    pub fn drop(&self, txn: &Transaction, del: bool) -> Result<()> {
        self.validate_write_transaction(txn)?;

        if del && self.dbi < CORE_DBS {
            return Err(Error::Incompatible);
        }

        if del {
            txn.delete_database(self.dbi).map_err(|e| match e {
                Error::BadTxn => Error::InvalidTxnState,
                Error::TxnFull => Error::TxnDirtyLimit,
                e => Error::DbOperationFailed
            })?;
        } else {
            txn.reset_database(self.dbi).map_err(|e| match e {
                Error::BadTxn => Error::InvalidTxnState,
                e => Error::DbOperationFailed
            })?;
        }

        Ok(())
    }

    /// Get a value by key.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for reading
    /// * `key` - Key to look up
    ///
    /// # Returns
    /// * `Ok(Some(value))` - Key was found, returns associated value
    /// * `Ok(None)` - Key was not found
    /// * `Err(...)` - Error occurred during lookup
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid
    /// * `Error::BadValSize` - Key size is invalid
    pub fn get(&self, txn: &Transaction, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.validate_transaction(txn)?;
        self.validate_key_size(txn, key)?;

        let key_val = Value::new(key);
        txn.get_value(self.dbi, &key_val)
            .map_err(|e| match e {
                Error::NotFound => Error::NotFound,
                Error::BadValSize => Error::BadValSize,
                Error::BadTxn => Error::InvalidTxnState,
                e => Error::DbOperationFailed
            })
    }

    /// Put a key/value pair.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for writing
    /// * `key` - Key to store
    /// * `data` - Data to store
    /// * `flags` - Write flags (NOOVERWRITE, NODUPDATA, etc)
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid or read-only
    /// * `Error::KeyExist` - Key already exists (with NOOVERWRITE flag)
    /// * `Error::BadValSize` - Key/value size is invalid
    /// * `Error::TxnFull` - Transaction has too many dirty pages
    /// * `Error::MapFull` - Database is full
    ///
    /// # Thread Safety
    /// Requires a write transaction. Only one write transaction can be active at a time.
    pub fn put(&self, txn: &Transaction, key: &[u8], data: &[u8], flags: WriteFlags) -> Result<()> {
        self.validate_write_transaction(txn)?;
        self.validate_key_size(txn, key)?;
        self.validate_flags(flags)?;

        let key_val = Value::new(key);
        let data_val = Value::new(data);
        
        txn.put_value(self.dbi, &key_val, &data_val, flags.bits())
            .map_err(|e| match e {
                Error::MapFull => Error::DirtyPagesExceeded,
                Error::TxnFull => Error::TxnDirtyLimit,
                Error::KeyExist if flags.contains(WriteFlags::NOOVERWRITE) => Error::KeyExist,
                Error::BadTxn => Error::InvalidTxnState,
                Error::BadValSize => Error::BadValSize,
                e => Error::DbOperationFailed
            })
    }

    /// Delete a key/value pair.
    ///
    /// # Arguments
    /// * `txn` - Transaction to use for deletion
    /// * `key` - Key to delete
    /// * `data` - For databases with DUPSORT flag, specifies which data value to delete.
    ///           If None, deletes all values for the key.
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::BadTxn` - Transaction is invalid or read-only
    /// * `Error::NotFound` - Key/data pair not found
    /// * `Error::BadValSize` - Key size is invalid
    ///
    /// # Thread Safety
    /// Requires a write transaction. Only one write transaction can be active at a time.
    pub fn del(&self, txn: &Transaction, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        self.validate_write_transaction(txn)?;
        self.validate_key_size(txn, key)?;

        if data.is_some() && !self.flags.contains(DbFlags::DUPSORT) {
            return Err(Error::Incompatible);
        }

        let key_val = Value::new(key);
        let data_val = data.map(Value::new);
        
        txn.del_value(self.dbi, &key_val, data_val.as_ref())
            .map_err(|e| match e {
                Error::NotFound => Error::NotFound,
                Error::BadValSize => Error::BadValSize,
                Error::BadTxn => Error::InvalidTxnState,
                Error::TxnFull => Error::TxnDirtyLimit,
                e => Error::DbOperationFailed
            })
    }

    /// Create a cursor for this database.
    ///
    /// # Returns
    /// A new cursor for navigating the database.
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::CursorFull` - Too many cursors already open
    ///
    /// # Thread Safety
    /// Cursors inherit the thread safety properties of their parent transaction.
    /// Multiple cursors can be created from the same transaction.
    pub fn cursor(&self) -> Result<Cursor> {
        self.validate_db_state()?;
        
        if !self.txn.as_ref().is_valid() {
            return Err(Error::InvalidTxnState);
        }

        self.validate_cursor_state(self.txn.as_ref())?;

        Ok(Cursor {
            txn: self.txn,
            dbi: self.dbi,
            page: None,
            pos: 0,
            stack: Vec::new(),
            flags: CursorFlags::empty(),
            db: self as *const _,
            xcursor: None,
        })
    }

    /// Set custom comparison function for keys.
    ///
    /// # Arguments
    /// * `cmp` - Comparison function that defines the desired key ordering
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    ///
    /// # Safety
    /// The comparison function must define a total ordering and must be consistent
    /// across all operations on the database. Changing the comparison function while
    /// the database contains data may lead to undefined behavior.
    pub fn set_compare<F>(&self, cmp: F) -> Result<()>
    where F: Fn(&[u8], &[u8]) -> Ordering + 'static {
        if self.dbi == 0 {
            return Err(Error::BadDbi);
        }

        if !self.txn.as_ref().is_valid() {
            return Err(Error::InvalidTxnState);
        }

        self.info.try_borrow_mut()
            .map_err(|_| Error::ResourceCleanupFailed)?
            .cmp = Some(Box::new(cmp));
        Ok(())
    }

    /// Set custom comparison function for duplicate values.
    ///
    /// # Arguments
    /// * `cmp` - Comparison function that defines the desired ordering for duplicate values
    ///
    /// # Errors
    /// * `Error::Invalid` - Invalid database handle
    /// * `Error::Incompatible` - Database was not opened with DUPSORT flag
    ///
    /// # Safety
    /// The comparison function must define a total ordering and must be consistent
    /// across all operations on the database. Changing the comparison function while
    /// the database contains data may lead to undefined behavior.
    pub fn set_dupsort<F>(&self, cmp: F) -> Result<()>
    where F: Fn(&[u8], &[u8]) -> Ordering + 'static {
        if !self.flags.contains(DbFlags::DUPSORT) {
            return Err(Error::Incompatible);
        }
        if self.dbi == 0 {
            return Err(Error::BadDbi);
        }

        if !self.txn.as_ref().is_valid() {
            return Err(Error::InvalidTxnState);
        }

        self.info.try_borrow_mut()
            .map_err(|_| Error::ResourceCleanupFailed)?
            .dupsort = Some(Box::new(cmp));
        Ok(())
    }

    // Add internal helper method for validation checks
    fn validate_transaction(&self, txn: &Transaction) -> Result<()> {
        if self.dbi == 0 {
            return Err(Error::BadDbi);
        }

        if !txn.is_valid() {
            return Err(Error::InvalidTxnState);
        }

        if !self.txn.as_ref().is_same_env(txn) {
            return Err(Error::Incompatible);
        }

        Ok(())
    }

    // Add helper for write transaction validation
    fn validate_write_transaction(&self, txn: &Transaction) -> Result<()> {
        self.validate_transaction(txn)?;

        if txn.is_readonly() {
            return Err(Error::TxnReadOnlyOp);
        }

        Ok(())
    }

    // Add helper for key/data validation
    fn validate_key_size(&self, txn: &Transaction, key: &[u8]) -> Result<()> {
        let max_size = txn.max_key_size();
        if key.len() > max_size {
            return Err(Error::BadValSize);
        }
        Ok(())
    }

    // Add validation for database state
    fn validate_db_state(&self) -> Result<()> {
        if self.dbi == 0 {
            return Err(Error::BadDbi);
        }

        if self.txn.is_null() {
            return Err(Error::InvalidTxnState);
        }

        if let Err(_) = self.info.try_borrow() {
            return Err(Error::ResourceCleanupFailed);
        }
        if let Err(_) = self.stats.try_borrow() {
            return Err(Error::ResourceCleanupFailed);
        }

        Ok(())
    }

    // Add validation for database flags
    fn validate_flags(&self, flags: WriteFlags) -> Result<()> {
        if flags.contains(WriteFlags::NODUPDATA) && !self.flags.contains(DbFlags::DUPSORT) {
            return Err(Error::Incompatible);
        }
        if flags.contains(WriteFlags::APPENDDUP) && !self.flags.contains(DbFlags::DUPSORT) {
            return Err(Error::Incompatible);
        }
        if flags.contains(WriteFlags::MULTIPLE) && flags.contains(WriteFlags::APPEND) {
            return Err(Error::Incompatible);
        }
        Ok(())
    }

    fn validate_cursor_state(&self, txn: &Transaction) -> Result<()> {
        let cursor_count = txn.cursor_count();
        let max_cursors = txn.max_cursors();
        
        if cursor_count >= max_cursors {
            return Err(Error::CursorFull);
        }

        if txn.has_active_cursors(self.dbi) && txn.is_finished() {
            return Err(Error::InvalidStateTransition);
        }

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            match e {
                Error::BadDbi => (), // Already closed
                Error::InvalidTxnState | Error::ResourceCleanupFailed => self.cleanup_resources(),
                _ => self.cleanup_resources(),
            }
        }
        self.txn = NonNull::dangling();
    }
}

impl Database {
    // Add helper method for cleanup during drop
    fn cleanup_resources(&mut self) {
        if let Ok(mut info) = self.info.try_borrow_mut() {
            info.cmp = None;
            info.dupsort = None;
            info.rel = None;
            info.relctx = None;
        }
        
        if let Ok(mut stats) = self.stats.try_borrow_mut() {
            *stats = DbStats::default();
        }

        self.dbi = 0;
        self.flags = DbFlags::empty();
    }
}
