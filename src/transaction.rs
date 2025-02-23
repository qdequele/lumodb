use std::any::Any;
use std::ffi::CString;
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::constants::{DbFlags, PageFlags, TransactionFlags};
use crate::cursor::{Cursor, Page};
use crate::database::{Database, DbStats};
use crate::env::Environment;
use crate::error::{Error, Result};

/// Database transaction matching LMDB C implementation
#[derive(Debug)]
pub struct Transaction<'env> {
    /// Reference to environment
    pub(crate) env: &'env Environment<'env>,
    /// Parent transaction for nested txns
    pub(crate) parent: Option<NonNull<Transaction<'env>>>,
    /// Child transaction
    pub(crate) child: Option<NonNull<Transaction<'env>>>,
    /// Transaction flags
    pub(crate) flags: TransactionFlags,
    /// Transaction ID
    pub(crate) txn_id: u64,
    /// Next page number
    pub(crate) next_pgno: u64,
    /// Last written page
    pub(crate) last_pgno: u64,
    /// Database handles
    pub(crate) dbs: Vec<Database<'env>>,
    /// Open cursors in this transaction
    pub(crate) cursors: Vec<Cursor<'env>>,
    /// Free pages list
    pub(crate) free_pgs: Vec<u64>,
    /// Loose pages list
    pub(crate) loose_pgs: Vec<u64>,
    /// Dirty pages list
    pub(crate) dirty_pages: Vec<u64>,
    /// Spill pages list
    pub(crate) spill_pages: Vec<u64>,
    /// Number of loose pages
    pub(crate) loose_count: u32,
    /// Database flags
    pub(crate) db_flags: Vec<u32>,
    /// Database sequences
    pub(crate) db_seqs: Vec<u32>,
    /// User context
    pub(crate) userctx: Option<Box<dyn Any + Send + Sync>>,
}

impl<'env> Transaction<'env> {
    /// Create a new transaction
    pub(crate) fn new(env: &'env Environment, readonly: bool) -> Result<Self> {
        let flags = if readonly {
            TransactionFlags::RDONLY
        } else {
            TransactionFlags::empty()
        };

        let txn_id = env.current_txn_id()?;

        let txn = Transaction {
            env,
            parent: None,
            child: None,
            flags,
            txn_id,
            next_pgno: 0,
            last_pgno: 0,
            dbs: Vec::new(),
            cursors: Vec::new(),
            free_pgs: Vec::new(),
            loose_pgs: Vec::new(),
            dirty_pages: Vec::new(),
            spill_pages: Vec::new(),
            loose_count: 0,
            db_flags: Vec::new(),
            db_seqs: Vec::new(),
            userctx: None,
        };

        if !readonly {
            env.register_reader(txn_id)?;
        }

        Ok(txn)
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(Error::Invalid);
        }

        if self.is_readonly() {
            return Ok(());
        }

        if !self.dirty_pages.is_empty()
            && self.dirty_pages.len() > self.env.get_max_dirty() as usize
        {
            self.spill()?;
        }

        if let Some(map) = &self.env.map {
            for &page in self.dirty_pages.iter() {
                let offset = page as usize * self.env.page_size;
                let data = &self.dirty_pages[..self.env.page_size];
                unsafe {
                    let ptr = map.as_ptr().add(offset);
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr() as *const u8,
                        ptr as *mut u8,
                        self.env.page_size,
                    );
                }
            }

            for &page in self.spill_pages.iter() {
                let offset = page as usize * self.env.page_size;
                let data = &self.spill_pages[..self.env.page_size];
                unsafe {
                    let ptr = map.as_ptr().add(offset);
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr() as *const u8,
                        ptr as *mut u8,
                        self.env.page_size,
                    );
                }
            }
        }

        Ok(())
    }

    /// Abort the transaction
    pub fn abort(&mut self) {
        // Clean up resources
        self.cursors.clear();
        self.dbs.clear();
        self.dirty_pages.clear();
        self.spill_pages.clear();

        // Mark transaction as finished
        self.flags |= TransactionFlags::FINISHED;
    }

    /// Reset the transaction
    pub fn reset(&mut self) {
        // Clean up resources
        self.cursors.clear();
        self.dbs.clear();
        self.dirty_pages.clear();
        self.spill_pages.clear();

        // Mark transaction as finished
        self.flags |= TransactionFlags::FINISHED;
    }

    /// Renew the transaction
    pub fn renew(&mut self) -> Result<()> {
        if !self.is_readonly() {
            return Err(Error::EnvReadOnly);
        }

        if !self.flags.contains(TransactionFlags::FINISHED) {
            return Err(Error::Invalid);
        }

        self.flags = TransactionFlags::RDONLY;
        self.txn_id = self.env.current_txn_id()?;
        self.env.register_reader(self.txn_id)?;

        Ok(())
    }

    /// Open a database
    pub(crate) fn open_db(&mut self, _name: &[u8], flags: DbFlags) -> Result<u32> {
        if self.dbs.len() >= self.env.max_dbs as usize {
            return Err(Error::DbsFull);
        }

        let dbi = self.dbs.len() as u32;
        let txn_ptr = NonNull::new(self as *mut _).unwrap();
        self.dbs.push(Database {
            txn: txn_ptr,
            dbi,
            flags,
            info: Default::default(),
            stats: Default::default(),
            _marker: PhantomData,
        });

        // Store database flags
        let flags = flags.bits();
        self.db_flags.push(flags);

        Ok(dbi)
    }

    /// Set comparison function
    pub(crate) fn set_compare(
        &mut self,
        dbi: u32,
        cmp: Box<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
    ) -> Result<()> {
        if dbi as usize >= self.dbs.len() {
            return Err(Error::BadDbi);
        }

        // Store the comparison function in the database
        self.dbs[dbi as usize].info.cmp = Some(cmp);
        Ok(())
    }

    /// Set duplicate sort function
    pub(crate) fn set_dupsort(
        &mut self,
        dbi: u32,
        dupsort: Box<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
    ) -> Result<()> {
        if dbi as usize >= self.dbs.len() {
            return Err(Error::BadDbi);
        }

        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Get mutable reference to database
        let db = &mut self.dbs[dbi as usize];

        // Check if database supports duplicates
        if !db.flags.contains(DbFlags::DUPSORT) {
            return Err(Error::Incompatible);
        }

        // Store the duplicate sort function in the database
        db.info.dupsort = Some(dupsort);

        Ok(())
    }

    /// Get value
    pub(crate) fn get_value(&self, dbi: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if dbi as usize >= self.dbs.len() {
            return Err(Error::BadDbi);
        }

        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Create cursor to search for key
        let cursor = self.cursors.get(dbi as usize).ok_or(Error::CursorFull)?;

        // Position cursor at key
        match cursor.set(key)? {
            Some((found_key, found_value)) => {
                if found_key == key {
                    Ok(Some(found_value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Put value
    pub(crate) fn put_value(
        &mut self,
        dbi: u32,
        key: &[u8],
        data: &[u8],
        flags: u32,
    ) -> Result<()> {
        if dbi as usize >= self.dbs.len() {
            return Err(Error::BadDbi);
        }

        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Create cursor to insert value
        let cursor = self
            .cursors
            .get_mut(dbi as usize)
            .ok_or(Error::CursorFull)?;

        // Check if key already exists
        match cursor.set(key)? {
            Some((_found_key, _)) => {
                // Key exists, check flags
                if flags & DbFlags::NOOVERWRITE.bits() != 0 {
                    return Err(Error::KeyExists);
                }
                // Update existing value
                cursor.put(key, data, flags)?;
            }
            None => {
                // Key doesn't exist, insert new value
                cursor.put(key, data, flags)?;
            }
        }

        // Track page as dirty
        if let Some(page) = cursor.get_page() {
            self.dirty_pages.push(page);
        }

        Ok(())
    }

    /// Delete value
    pub(crate) fn del_value(&mut self, dbi: u32, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        if dbi as usize >= self.dbs.len() {
            return Err(Error::BadDbi);
        }

        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Create cursor to delete value
        let cursor = self
            .cursors
            .get_mut(dbi as usize)
            .ok_or(Error::CursorFull)?;

        // Find the key to delete
        match cursor.set(key)? {
            Some((_found_key, found_value)) => {
                // Key exists, check if data matches if provided
                if let Some(expected_data) = data {
                    if found_value != expected_data {
                        return Err(Error::NotFound);
                    }
                }
                // Delete the key-value pair
                cursor.del(0)?;

                // Track page as dirty
                if let Some(page) = cursor.get_page() {
                    self.dirty_pages.push(page);
                }

                Ok(())
            }
            None => Err(Error::NotFound),
        }
    }

    /// Check for active cursors
    pub(crate) fn has_active_cursors(&self, dbi: u32) -> bool {
        self.cursors.iter().any(|c| c.dbi == dbi && !c.is_unused())
    }

    /// Check if transaction is valid
    pub fn is_valid(&self) -> bool {
        !self
            .flags
            .contains(TransactionFlags::FINISHED | TransactionFlags::ERROR)
    }

    /// Check if transaction is read-only
    pub fn is_readonly(&self) -> bool {
        self.flags.contains(TransactionFlags::RDONLY)
    }

    /// Spill dirty pages
    fn spill(&mut self) -> Result<()> {
        if self.dirty_pages.is_empty() {
            return Ok(());
        }

        let need = self.dirty_pages.len() - self.env.get_max_dirty() as usize;
        if need == 0 {
            return Ok(());
        }

        // Move oldest dirty pages to spill pages
        let spill_start = self.spill_pages.len();
        self.spill_pages
            .extend_from_slice(&self.dirty_pages[..need]);
        self.dirty_pages.drain(..need);

        if let Some(map) = &self.env.map {
            for &page in self.spill_pages[spill_start..].iter() {
                let offset = page as usize * self.env.page_size;
                let data = &self.spill_pages[spill_start..spill_start + 1];
                unsafe {
                    let ptr = map.as_ptr().add(offset);
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr() as *const u8,
                        ptr as *mut u8,
                        self.env.page_size,
                    );
                }
            }
        }

        Ok(())
    }

    /// Get transaction ID
    pub fn id(&self) -> u64 {
        self.txn_id
    }

    /// Get environment reference
    pub fn env(&self) -> &Environment {
        self.env
    }

    /// Check if transaction is finished
    pub fn is_finished(&self) -> bool {
        self.flags.contains(TransactionFlags::FINISHED)
    }

    /// Check if transaction has child
    pub fn has_child(&self) -> bool {
        self.flags.contains(TransactionFlags::HAS_CHILD)
    }

    /// Initialize a read-only transaction
    fn init_read_txn(txn: &mut Transaction, env: &Environment) -> Result<()> {
        txn.txn_id = env.current_txn_id()?;
        env.register_reader(txn.txn_id)
    }

    /// Initialize a write transaction
    fn init_write_txn(txn: &mut Transaction, env: &Environment) -> Result<()> {
        if env.has_active_write_txn() {
            return Err(Error::TxnInvalid);
        }

        if env.is_readonly() {
            return Err(Error::EnvReadOnly);
        }

        // Validate page boundaries
        let next_page = env.next_page();
        let last_page = env.last_page();

        if next_page >= last_page {
            return Err(Error::MapFull);
        }

        txn.txn_id = env.next_txn_id();
        txn.next_pgno = next_page;
        txn.last_pgno = last_page;
        env.set_active_write_txn(true);
        Ok(())
    }

    fn inherit_from_parent(&mut self, parent: &Transaction<'env>) -> Result<()> {
        // Validate parent state
        if parent.is_finished() {
            return Err(Error::BadTxn);
        }

        if parent.flags().contains(TransactionFlags::ERROR) {
            return Err(Error::BadTxn);
        }

        // Validate database limits
        if parent.dbs.len() > self.env.max_dbs as usize {
            return Err(Error::DbsFull);
        }

        // Copy parent's database state
        self.dbs = parent.dbs.clone();
        self.db_flags = parent.db_flags.clone();

        // Share read-only resources
        self.txn_id = parent.txn_id;
        self.next_pgno = parent.next_pgno;
        self.last_pgno = parent.last_pgno;

        // Initialize separate dirty page tracking
        self.dirty_pages = Vec::new();
        self.spill_pages = Vec::new();

        Ok(())
    }

    fn register_reader(&self) -> Result<()> {
        if self.is_readonly() {
            if self
                .env
                .num_readers
                .load(std::sync::atomic::Ordering::Acquire)
                >= self.env.get_max_readers()
            {
                return Err(Error::ReadersFull);
            }
            self.env.register_reader(self.txn_id)
        } else {
            Ok(())
        }
    }

    fn release_reader(&self) {
        if self.is_readonly() {
            // Release reader slot
            self.env.release_reader(self.txn_id);
        }
    }

    fn check_state(&self) -> Result<()> {
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        if self.flags().contains(TransactionFlags::ERROR) {
            return Err(Error::BadTxn);
        }

        if self.has_child() {
            return Err(Error::TxnHasChild);
        }

        if !self.env.is_initialized() {
            return Err(Error::EnvNotInitialized);
        }

        Ok(())
    }

    fn allocate_page(&mut self) -> Result<u64> {
        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }
        if self.is_readonly() {
            return Err(Error::TxnReadOnly);
        }

        if self.next_pgno >= self.last_pgno {
            return Err(Error::MapFull);
        }

        let page_no = self.next_pgno;
        self.next_pgno += 1;

        // Track as dirty
        self.dirty_pages.push(page_no);

        Ok(page_no)
    }

    fn free_page(&mut self, page_no: u64) {
        self.free_pgs.push(page_no);
    }

    fn merge_child_txn(&mut self, child: Transaction<'env>) -> Result<()> {
        // Validate states before merge
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }
        if !child.is_finished() {
            return Err(Error::BadTxn);
        }

        // Check capacity for merging pages
        if self.dirty_pages.len() + child.dirty_pages.len() > self.env.get_max_dirty() {
            return Err(Error::DirtyPagesExceeded);
        }

        // Merge dirty pages
        self.dirty_pages.extend(child.dirty_pages.iter().cloned());

        // Merge spill pages
        if child.flags().contains(TransactionFlags::SPILLS) {
            self.spill_pages.extend(child.spill_pages.iter().cloned());
            self.add_flags(TransactionFlags::SPILLS);
        }

        // Update database states
        self.dbs = child.dbs.clone();
        self.db_flags = child.db_flags.clone();

        // Update transaction state
        self.next_pgno = child.next_pgno;
        self.last_pgno = child.last_pgno;
        self.remove_flags(TransactionFlags::HAS_CHILD);

        Ok(())
    }

    fn handle_loose_page(&mut self, page: &Page) -> Result<()> {
        if self.is_readonly() {
            return Err(Error::TxnReadOnly);
        }

        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        if page.number() >= self.last_pgno {
            return Err(Error::PageNotFound);
        }

        if page.flags().contains(PageFlags::DIRTY) {
            // Check if we can handle another dirty page
            if self.dirty_pages.len() >= self.env.get_max_dirty() {
                return Err(Error::DirtyPagesExceeded);
            }
            self.loose_pgs.push(page.number());
        } else {
            self.free_pgs.push(page.number());
        }
        Ok(())
    }

    // Add validation for transaction boundaries
    fn validate_transaction_limits(&self) -> Result<()> {
        if self.dirty_pages.len() >= self.env.get_max_dirty() as usize {
            return Err(Error::TxnDirtyLimit);
        }

        if self.cursors.len() >= self.env.get_max_cursors() as usize {
            return Err(Error::TxnCursorLimit);
        }

        if self.dbs.len() >= self.env.get_max_dbs() as usize {
            return Err(Error::TxnDbLimit);
        }

        Ok(())
    }

    // Add validation for database operations
    fn validate_db_operation(&self, db: &Database) -> Result<()> {
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        if !self.dbs.contains(db) {
            return Err(Error::DbNotFound);
        }

        if self.is_readonly() && !db.flags()?.contains(DbFlags::RDONLY) {
            return Err(Error::TxnReadOnlyOp);
        }

        Ok(())
    }

    // Enhanced error recovery
    fn recover_from_error(&mut self, error: &Error) -> Result<()> {
        // Set error flag
        self.add_flags(TransactionFlags::ERROR);

        match error {
            Error::MapFull | Error::DirtyPagesExceeded => {
                // Try to spill pages if we exceed max dirty pages
                if self.dirty_pages.len() >= self.env.get_max_dirty() {
                    self.spill()?;
                }
            }
            Error::CursorFull => {
                // Try to close unused cursors
                self.cleanup_unused_cursors()?;
            }
            Error::PageCorrupted => {
                // Log corruption and mark page as bad
                self.mark_page_corrupted()?;
            }
            _ => {
                // For other errors, abort transaction
                self.abort();
                return Err(Error::RecoveryFailed);
            }
        }

        Ok(())
    }

    // Add cleanup for unused cursors
    fn cleanup_unused_cursors(&mut self) -> Result<()> {
        // First collect indices of unused cursors
        let to_remove: Vec<usize> = self
            .cursors
            .iter()
            .enumerate()
            .filter(|(_, cursor)| cursor.is_unused())
            .map(|(idx, _)| idx)
            .collect();

        // Then close and remove cursors in reverse order
        for idx in to_remove.iter().rev() {
            // Get mutable reference to cursor before closing
            if let Some(cursor) = self.cursors.get_mut(*idx) {
                cursor.close()?;
            }
            self.cursors.remove(*idx);
        }

        Ok(())
    }

    // Add corrupted page handling
    fn mark_page_corrupted(&mut self) -> Result<()> {
        if self.is_readonly() {
            return Err(Error::TxnReadOnly);
        }

        // Log corruption in metadata
        self.add_flags(TransactionFlags::CORRUPTED);

        Ok(())
    }

    // Enhanced Drop implementation
    fn safe_cleanup(&mut self) -> Result<()> {
        // Ensure all cursors are closed
        for cursor in &mut self.cursors {
            if let Err(_e) = cursor.close() {
                return Err(Error::CursorCleanupFailed);
            }
        }
        self.cursors.clear();

        // Ensure all dirty pages are handled
        if !self.is_readonly() {
            let pages: Vec<_> = self.dirty_pages.iter().copied().collect();
            for page in pages {
                if let Err(_e) = self.handle_loose_page(&Page::from_number(page)) {
                    return Err(Error::PageCleanupFailed);
                }
            }
        }

        // Release reader slot if needed
        self.release_reader();

        // Abort the transaction
        self.abort();

        Ok(())
    }

    // Add isolation level validation
    fn validate_isolation_level(&self, operation: &str) -> Result<()> {
        let current_level = self.flags().intersection(
            TransactionFlags::RDONLY | TransactionFlags::WRITEMAP | TransactionFlags::NOSYNC,
        );

        match operation {
            "write" if current_level.contains(TransactionFlags::RDONLY) => {
                return Err(Error::TxnIsolationConflict)
            }
            "snapshot" if !current_level.contains(TransactionFlags::RDONLY) => {
                return Err(Error::TxnIsolationConflict)
            }
            _ => Ok(()),
        }
    }

    // Add error context tracking
    fn track_error(&mut self, error: Error, _context: &str) -> Error {
        self.add_flags(TransactionFlags::ERROR);

        // For now, just return the error since we don't have a logger
        error
    }

    // Add transaction retry mechanism
    fn retry_operation<F, T>(&mut self, retries: u32, op: F) -> Result<T>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < retries {
            match op(self) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    attempts += 1;
                }
            }
        }

        Err(Error::TxnRetryExhausted(Box::new(
            last_error.unwrap_or(Error::Unknown),
        )))
    }

    // Add deadlock detection
    fn check_deadlock(&self) -> Result<()> {
        if let Some(parent) = &self.parent {
            let mut current = unsafe { parent.as_ref() };
            while let Some(p) = &current.parent {
                let parent_txn = unsafe { p.as_ref() };
                if std::ptr::eq(parent_txn as *const _, self as *const _) {
                    return Err(Error::TxnDeadlock);
                }
                current = parent_txn;
            }
        }
        Ok(())
    }

    // Add transaction fence validation
    fn validate_fence(&self, fence_txn_id: u64) -> Result<()> {
        if self.txn_id < fence_txn_id {
            return Err(Error::TxnFenceConflict);
        }
        Ok(())
    }

    // Enhanced error propagation
    fn propagate_error(&mut self, error: Error) -> Result<()> {
        // Mark transaction as errored
        self.add_flags(TransactionFlags::ERROR);

        // Propagate error to parent transaction if it exists
        if let Some(parent) = self.parent {
            unsafe {
                (*parent.as_ptr()).add_flags(TransactionFlags::ERROR);
            }
        }

        // Attempt recovery if possible
        match error {
            Error::TxnFull | Error::MapResized | Error::PageCorrupted => {
                self.recover_from_error(&error)
            }
            _ => Err(error),
        }
    }

    pub(crate) fn max_key_size(&self) -> usize {
        self.env.get_max_key_size() as usize
    }

    pub(crate) fn cursor_count(&self) -> usize {
        self.cursors.len()
    }

    pub(crate) fn max_cursors(&self) -> usize {
        self.env.get_max_cursors() as usize
    }

    pub(crate) fn allocate_dbi(&mut self) -> Result<u32> {
        if self.dbs.len() >= self.env.get_max_dbs() as usize {
            return Err(Error::DbsFull);
        }
        Ok(self.dbs.len() as u32 + 2) // 0 and 1 are reserved
    }

    pub(crate) fn register_database(
        &mut self,
        db: &Database<'env>,
        name: Option<&CString>,
    ) -> Result<()> {
        if self.dbs.len() >= self.env.get_max_dbs() as usize {
            return Err(Error::DbsFull);
        }

        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Check if database already exists
        if self.dbs.contains(db) {
            return Ok(());
        }

        // Add database to transaction's database list
        self.dbs.push(db.clone());

        // Store database flags
        let flags = db.flags()?.bits();
        self.db_flags.push(flags);

        Ok(())
    }

    pub(crate) fn get_db_stats(&self, dbi: u32) -> Result<DbStats> {
        if dbi >= self.dbs.len() as u32 {
            return Err(Error::BadDbi);
        }

        // Get database reference
        let db = &self.dbs[dbi as usize];

        // Validate database operation
        self.validate_db_operation(db)?;

        // Get root page number
        let root_pgno = db.root_page()?;
        if root_pgno == 0 {
            // Empty database
            return Ok(DbStats::default());
        }

        let mut stats = DbStats::default();

        // Get page from root
        let root_page = self.get_page(root_pgno)?;

        // Count pages by type
        let page = unsafe { root_page.as_ref() };
        match page.flags() {
            flags if flags.contains(PageFlags::BRANCH) => {
                stats.branch_pages += 1;
            }
            flags if flags.contains(PageFlags::LEAF) => {
                stats.leaf_pages += 1;
            }
            flags if flags.contains(PageFlags::OVERFLOW) => {
                stats.overflow_pages += 1;
            }
            _ => return Err(Error::Corrupted),
        }

        // Set depth and entry count
        stats.depth = page.level();
        stats.entries = page.entries();

        Ok(stats)
    }

    pub(crate) fn delete_database(&mut self, dbi: u32) -> Result<()> {
        if self.is_readonly() {
            return Err(Error::TxnReadOnly);
        }
        if dbi >= self.dbs.len() as u32 {
            return Err(Error::BadDbi);
        }

        // Get database reference
        let db = &self.dbs[dbi as usize];

        // Validate database operation
        self.validate_db_operation(db)?;

        // Get root page number
        let root_pgno = db.root_page()?;
        if root_pgno == 0 {
            // Empty database, nothing to delete
            return Ok(());
        }

        // Free all pages starting from root
        let root_page = self.get_page(root_pgno)?;
        self.free_page(root_pgno);

        // If branch page, recursively free all child pages
        let page = unsafe { root_page.as_ref() };
        if page.flags().contains(PageFlags::BRANCH) {
            for i in 0..page.entries() {
                let child_pgno = page.get_branch_pgno(i)?;
                self.free_page(child_pgno);
            }
        }

        // Clear database state
        self.dbs[dbi as usize] = Database::default();

        Ok(())
    }

    pub(crate) fn reset_database(&mut self, dbi: u32) -> Result<()> {
        if self.is_readonly() {
            return Err(Error::TxnReadOnly);
        }
        if dbi >= self.dbs.len() as u32 {
            return Err(Error::BadDbi);
        }

        // Get database reference
        let db = &self.dbs[dbi as usize];

        // Validate database operation
        self.validate_db_operation(db)?;

        // Get root page number
        let root_pgno = db.root_page()?;
        if root_pgno == 0 {
            // Empty database, nothing to reset
            return Ok(());
        }

        // Free all pages starting from root
        let root_page = self.get_page(root_pgno)?;
        self.free_page(root_pgno);

        // If branch page, recursively free all child pages
        let page = unsafe { root_page.as_ref() };
        if page.flags().contains(PageFlags::BRANCH) {
            for i in 0..page.entries() {
                let child_pgno = page.get_branch_pgno(i)?;
                self.free_page(child_pgno);
            }
        }

        // Reset database root page to 0 but keep other settings
        let mut db = self.dbs[dbi as usize].clone();
        db.set_root_page(0)?;
        self.dbs[dbi as usize] = db;

        Ok(())
    }

    pub(crate) fn to_internal_flags(flags: TransactionFlags) -> u32 {
        flags.bits()
    }

    pub(crate) fn is_same_env(&self, other: &Transaction<'env>) -> bool {
        std::ptr::eq(self.env, other.env)
    }

    pub(crate) fn is_null(&self) -> bool {
        self.env as *const Environment == std::ptr::null()
    }

    pub(crate) fn flags(&self) -> TransactionFlags {
        self.flags
    }

    pub(crate) fn add_flags(&mut self, flags: TransactionFlags) {
        self.flags |= flags;
    }

    pub(crate) fn remove_flags(&mut self, flags: TransactionFlags) {
        self.flags &= !flags;
    }

    pub(crate) fn set_flags(&mut self, flags: TransactionFlags) {
        self.flags = flags;
    }

    pub(crate) fn get_page(&self, pgno: u64) -> Result<NonNull<Page>> {
        // Validate transaction state
        if !self.is_valid() {
            return Err(Error::InvalidTxnState);
        }

        // Validate page number
        if pgno >= self.last_pgno {
            return Err(Error::PageNotFound);
        }

        // Get page from environment memory map
        let page = self.env.get_page(pgno)?;

        // For write transactions, check if page is dirty and get from dirty page cache
        if !self.is_readonly() {
            if let Some(dirty_page) = self.dirty_pages.iter().find(|&&p| p == pgno) {
                // Get page from dirty page cache
                return self.env.get_dirty_page(*dirty_page);
            }
        }

        Ok(page)
    }
}

impl<'env> Drop for Transaction<'env> {
    fn drop(&mut self) {
        if !self.is_finished() {
            self.reset();
        }
    }
}

impl<'env> PartialEq for Transaction<'env> {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id == other.txn_id && std::ptr::eq(self.env, other.env)
    }
}
