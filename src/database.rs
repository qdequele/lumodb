use crate::constants::{
    CursorFlags,
    // Flags
    DbFlags,
    WriteFlags,
    // Global constants
    CORE_DBS,
};
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::transaction::Transaction;
use std::any::Any;
use std::cmp::Ordering;
use std::ffi::CString;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

/// Database statistics
#[derive(Debug, Clone, Default, PartialEq)]
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
pub struct DatabaseInfo {
    pub(crate) flags: DbFlags,
    pub(crate) name: Option<String>,
    pub(crate) cmp: Option<Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>>,
    pub(crate) dupsort: Option<Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>>,
    pub(crate) rel: Option<Box<dyn Fn(&[u8], &[u8]) -> bool + Send + Sync>>,
    pub(crate) dbi: u32,
    pub(crate) key_size: usize,
    pub(crate) depth: u16,
    pub(crate) relctx: Option<Box<dyn Any + Send + Sync>>,
}

impl Default for DatabaseInfo {
    fn default() -> Self {
        Self {
            flags: DbFlags::empty(),
            name: None,
            cmp: None,
            dupsort: None,
            rel: None,
            dbi: 0,
            key_size: 0,
            depth: 0,
            relctx: None,
        }
    }
}

impl Clone for DatabaseInfo {
    fn clone(&self) -> Self {
        Self {
            flags: self.flags.clone(),
            name: self.name.clone(),
            cmp: None, // Function pointers can't be cloned, so we reset them
            dupsort: None,
            rel: None,
            dbi: self.dbi,
            key_size: self.key_size,
            depth: self.depth,
            relctx: None, // Any can't be cloned, so we reset it
        }
    }
}

impl std::fmt::Debug for DatabaseInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseInfo")
            .field("flags", &self.flags)
            .field("name", &self.name)
            .field("has_cmp", &self.cmp.is_some())
            .field("has_dupsort", &self.dupsort.is_some())
            .field("has_rel", &self.rel.is_some())
            .field("dbi", &self.dbi)
            .field("key_size", &self.key_size)
            .field("depth", &self.depth)
            .field("relctx", &self.relctx.is_some())
            .finish()
    }
}

impl DatabaseInfo {
    pub fn new(dbi: u32, name: Option<String>, flags: DbFlags) -> Self {
        Self {
            flags,
            name,
            cmp: None,
            dupsort: None,
            rel: None,
            dbi,
            key_size: 0,
            depth: 0,
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
#[derive(Debug, Clone)]
pub struct Database<'env> {
    /// Reference to transaction
    pub(crate) txn: NonNull<Transaction<'env>>,
    /// Database identifier
    pub(crate) dbi: u32,
    /// Database flags
    pub(crate) flags: DbFlags,
    /// Database information
    pub(crate) info: DatabaseInfo,
    /// Database statistics
    pub(crate) stats: DbStats,
    /// Phantom data
    pub(crate) _marker: PhantomData<&'env ()>,
}

impl<'env> Database<'env> {
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
    pub fn open(
        txn: &'env mut Transaction<'env>,
        name: Option<&str>,
        flags: DbFlags,
    ) -> Result<Self> {
        if !txn.is_valid() {
            return Err(Error::Invalid);
        }

        let dbi = match name {
            Some(name) => {
                let cname = CString::new(name).map_err(|_| Error::InvalidParam)?;
                txn.open_db(cname.as_bytes_with_nul(), flags)?
            }
            None => txn.open_db(&[], flags)?,
        };

        Ok(Self {
            txn: NonNull::from(txn),
            dbi,
            flags,
            info: DatabaseInfo::new(dbi, name.map(String::from), flags),
            stats: DbStats::default(),
            _marker: PhantomData,
        })
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

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::InvalidTxnState);
            }
            if txn.has_active_cursors(self.dbi) {
                return Err(Error::ResourceCleanupFailed);
            }
        }

        self.info = DatabaseInfo::default();
        self.stats = DbStats::default();
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
    pub fn stat(&self, _txn: &Transaction) -> Result<DbStats> {
        Ok(self.stats.clone())
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
    pub fn flags(&self) -> Result<DbFlags> {
        unsafe {
            if !self.txn.as_ref().is_valid() {
                return Err(Error::Invalid);
            }
            Ok(self.flags.clone())
        }
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
    pub fn drop(&self, txn: &'env mut Transaction<'env>, del: bool) -> Result<()> {
        self.validate_write_transaction(txn)?;

        if del && self.dbi < CORE_DBS {
            return Err(Error::Incompatible);
        }

        if del {
            txn.delete_database(self.dbi).map_err(|e| match e {
                Error::BadTxn => Error::InvalidTxnState,
                Error::TxnFull => Error::TxnDirtyLimit,
                _e => Error::DbOperationFailed,
            })?;
        } else {
            txn.reset_database(self.dbi).map_err(|e| match e {
                Error::BadTxn => Error::InvalidTxnState,
                _e => Error::DbOperationFailed,
            })?;
        }

        Ok(())
    }

    /// Get value from the database
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.validate_db_state()?;

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::Invalid);
            }
            txn.get_value(self.dbi, key)
        }
    }

    /// Put a value into the database
    pub fn put(&mut self, key: &[u8], data: &[u8], flags: DbFlags) -> Result<()> {
        self.validate_db_state()?;

        unsafe {
            let txn = self.txn.as_mut();
            if !txn.is_valid() {
                return Err(Error::Invalid);
            }
            if txn.is_readonly() {
                return Err(Error::ReadOnly);
            }
            txn.put_value(self.dbi, key, data, flags.bits())
        }
    }

    /// Delete a value from the database
    pub fn del(&mut self, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        self.validate_db_state()?;

        unsafe {
            let txn = self.txn.as_mut();
            if !txn.is_valid() {
                return Err(Error::Invalid);
            }
            if txn.is_readonly() {
                return Err(Error::ReadOnly);
            }
            txn.del_value(self.dbi, key, data)
        }
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

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::InvalidTxnState);
            }

            self.validate_cursor_state(txn)?;
        }

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
    pub fn set_compare<F>(&mut self, cmp: F) -> Result<()>
    where
        F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    {
        self.validate_db_state()?;

        unsafe {
            let txn = self.txn.as_mut();
            if !txn.is_valid() {
                return Err(Error::Invalid);
            }
            if txn.is_readonly() {
                return Err(Error::ReadOnly);
            }
            if txn.has_active_cursors(self.dbi) {
                return Err(Error::CursorActive);
            }
            txn.set_compare(self.dbi, Box::new(cmp))
        }
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
    pub fn set_dupsort<F>(&mut self, dupsort: F) -> Result<()>
    where
        F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    {
        self.validate_db_state()?;

        unsafe {
            let txn = self.txn.as_mut();
            if !txn.is_valid() {
                return Err(Error::Invalid);
            }
            if txn.is_readonly() {
                return Err(Error::ReadOnly);
            }
            if txn.has_active_cursors(self.dbi) {
                return Err(Error::CursorActive);
            }
            txn.set_dupsort(self.dbi, Box::new(dupsort))
        }
    }

    // Add internal helper method for validation checks
    pub fn validate_transaction(&self, txn: &Transaction<'env>) -> Result<()> {
        if self.dbi == 0 {
            return Err(Error::BadDbi);
        }

        if !txn.is_valid() {
            return Err(Error::InvalidTxnState);
        }

        unsafe {
            if !self.txn.as_ref().is_valid() {
                return Err(Error::Incompatible);
            }
        }

        Ok(())
    }

    // Add helper for write transaction validation
    pub fn validate_write_transaction(&self, txn: &Transaction<'env>) -> Result<()> {
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

        unsafe {
            if !self.txn.as_ref().is_valid() {
                return Err(Error::InvalidTxnState);
            }
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

    // Add validation for cursor state
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

    /// Create a new database handle
    pub(crate) fn new(txn: NonNull<Transaction<'env>>, dbi: u32, flags: DbFlags) -> Self {
        Database {
            txn,
            dbi,
            flags,
            info: DatabaseInfo::default(),
            stats: DbStats::default(),
            _marker: PhantomData,
        }
    }

    pub(crate) fn root_page(&self) -> Result<u64> {
        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::InvalidTxnState);
            }
        }
        // For now return 0 - actual implementation would fetch from meta page
        Ok(0)
    }

    pub(crate) fn set_root_page(&mut self, pgno: u64) -> Result<()> {
        self.info.depth = 0;
        self.stats = DbStats::default();
        Ok(())
    }
}

impl<'env> Drop for Database<'env> {
    fn drop(&mut self) {
        self.cleanup_resources();
    }
}

impl<'env> Database<'env> {
    // Add helper method for cleanup during drop
    fn cleanup_resources(&mut self) {
        self.info.cmp = None;
        self.info.dupsort = None;
        self.info.rel = None;
        self.info.relctx = None;

        self.stats = DbStats::default();

        self.dbi = 0;
        self.flags = DbFlags::empty();
    }
}

impl<'env> PartialEq for Database<'env> {
    fn eq(&self, other: &Self) -> bool {
        self.dbi == other.dbi && unsafe { std::ptr::eq(self.txn.as_ptr(), other.txn.as_ptr()) }
    }
}

impl<'env> Default for Database<'env> {
    fn default() -> Self {
        Self {
            txn: NonNull::dangling(),
            dbi: 0,
            flags: DbFlags::empty(),
            info: DatabaseInfo::default(),
            stats: DbStats::default(),
            _marker: PhantomData,
        }
    }
}
