use std::os::raw::c_void;
use std::sync::atomic::{AtomicU32, Ordering};
use std::ptr::null_mut;
use std::ptr::NonNull;
use std::ptr::NonNull::new;
use std::ptr::NonNull::new_unchecked;

use crate::constants::{
    TransactionFlags, EnvFlags,
};
use crate::env::Environment;
use crate::error::{Error, Result};
use crate::cursor::Cursor;
use crate::database::Database;
use crate::page::Page;
use crate::page::PageFlags;

/// Database transaction
#[derive(Debug)]
pub(crate) struct Transaction<'env> {
    /// Reference to environment
    env: &'env Environment,
    /// Parent transaction for nested txns
    parent: Option<&'env Transaction<'env>>,
    /// Transaction flags 
    flags: AtomicU32,
    /// Transaction ID
    txn_id: u64,
    /// Open cursors in this transaction
    cursors: Vec<Cursor>,
    /// Database handles
    dbs: Vec<Database>,
    /// Database flags
    db_flags: Vec<u32>,
    /// Free pages
    free_pages: Vec<u64>,
    /// Dirty pages
    dirty_pages: Vec<u64>,
    /// Spill pages
    spill_pages: Vec<u64>,
    /// Root page number
    root_page: u32,
    /// Next page number
    next_page: u64,
    /// Last used page
    last_page: u64,
    /// User-provided context
    userctx: *mut c_void,
    /// Loose pages
    loose_pages: Vec<u64>,
}

impl<'env> Transaction<'env> {
    /// Create a new transaction
    pub(crate) fn new(env: &'env Environment, 
                      parent: Option<&'env Transaction>, 
                      flags: TransactionFlags) -> Result<Self> {
        // Add validation for environment state
        if !env.is_initialized() {
            return Err(Error::EnvNotInitialized);
        }

        // Validate parent transaction state if present
        if let Some(p) = parent {
            if p.is_finished() {
                return Err(Error::BadTxn);
            }
            if p.flags().contains(TransactionFlags::ERROR) {
                return Err(Error::BadTxn);
            }
            if flags.contains(TransactionFlags::RDONLY) {
                return Err(Error::Incompatible);
            }
        }

        // Initialize transaction struct
        let mut txn = Transaction {
            env,
            parent,
            flags: AtomicU32::new(0),
            txn_id: 0,
            cursors: Vec::new(),
            dbs: Vec::with_capacity(env.max_dbs()),
            db_flags: Vec::with_capacity(env.max_dbs()),
            free_pages: Vec::new(),
            dirty_pages: Vec::new(),
            spill_pages: Vec::new(),
            root_page: 0,
            next_page: 0,
            last_page: 0,
            userctx: null_mut(),
            loose_pages: Vec::new(),
        };

        // Select meta page based on transaction ID
        let meta_idx = txn.txn_id & 1;

        // Convert TransactionFlags to internal flags
        let flag_bits = flags.to_internal_flags();
        txn.flags.store(flag_bits, Ordering::SeqCst);

        if flags.contains(TransactionFlags::RDONLY) {
            Self::init_read_txn(&mut txn, env)?;
        } else {
            Self::init_write_txn(&mut txn, env)?;
        }

        Ok(txn)
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        // Enhance state validation
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }
        if self.flags().contains(TransactionFlags::ERROR) {
            return Err(Error::BadTxn);
        }

        if self.has_child() {
            return Err(Error::BadTxn);
        }

        // Add validation for dirty pages limit
        if !self.is_readonly() && self.dirty_pages.len() > self.env.max_dirty() {
            return Err(Error::TxnFull);
        }

        if self.is_readonly() {
            self.set_flags(TransactionFlags::FINISHED);
            return Ok(());
        }

        // Write to the alternate meta page
        let meta_idx = self.txn_id & 1;
        if let Err(e) = self.write_meta_page(meta_idx) {
            self.abort();
            return Err(Error::Panic);
        }

        // Commit all dirty pages to disk
        if let Some(map) = &self.env.map {
            // Write dirty pages
            for page in &self.dirty_pages {
                let offset = page * self.env.page_size as u64;
                let data = &self.dirty_pages[..self.env.page_size];
                map.flush_range(offset as usize, self.env.page_size)?;
            }

            // Handle spill pages
            if self.flags().contains(TransactionFlags::SPILLS) {
                // Write spill pages to disk
                for page in &self.spill_pages {
                    let offset = page * self.env.page_size as u64;
                    let data = &self.spill_pages[..self.env.page_size];
                    map.flush_range(offset as usize, self.env.page_size)?;
                }
                
                // Clear spill pages after writing
                self.spill_pages.clear();
            }

            // Sync if needed
            if !self.flags().intersects(TransactionFlags::NOSYNC | TransactionFlags::NOMETASYNC) {
                map.flush()?;
            }
        }

        // Mark as finished
        self.set_flags(TransactionFlags::FINISHED);
        self.dirty_pages.clear();
        self.free_pages.clear();
        self.spill_pages.clear();

        Ok(())
    }

    /// Abort the transaction
    pub fn abort(mut self) {
        // Check if transaction is still valid
        if self.flags().contains(TransactionFlags::FINISHED) {
            return;
        }

        // Abort any child transaction first
        if self.flags().contains(TransactionFlags::HAS_CHILD) {
            // Handle child abort without taking ownership
            if let Some(child) = self.parent {
                // Signal child to abort
                child.flags.fetch_or(TransactionFlags::ERROR.bits(), Ordering::SeqCst);
            }
        }

        // For read-only transaction, just reset state
        if self.flags().contains(TransactionFlags::RDONLY) {
            self.flags.store(TransactionFlags::FINISHED.bits(), Ordering::SeqCst);
            return;
        }

        // Clear all dirty pages
        self.dirty_pages.clear();
        self.free_pages.clear();
        self.spill_pages.clear();

        // Mark transaction as finished
        self.flags.store(TransactionFlags::FINISHED.bits() | TransactionFlags::ERROR.bits(), Ordering::SeqCst);

        // Release reader slot
        if let Some(reader) = self.env.num_readers.get_mut() {
            *reader -= 1;
        }
    }

    /// Get transaction ID
    pub fn id(&self) -> u64 {
        self.txn_id
    }

    /// Get reference to environment
    pub fn env(&self) -> &Environment {
        self.env
    }

    fn flags(&self) -> TransactionFlags {
        TransactionFlags::from_bits_truncate(self.flags.load(Ordering::SeqCst))
    }

    fn set_flags(&self, flags: TransactionFlags) {
        self.flags.store(flags.bits(), Ordering::SeqCst)
    }

    fn add_flags(&self, flags: TransactionFlags) {
        self.flags.fetch_or(flags.bits(), Ordering::SeqCst);
    }

    fn remove_flags(&self, flags: TransactionFlags) {
        self.flags.fetch_and(!flags.bits(), Ordering::SeqCst);
    }

    pub fn is_readonly(&self) -> bool {
        self.flags().contains(TransactionFlags::RDONLY)
    }

    pub fn is_finished(&self) -> bool {
        self.flags().contains(TransactionFlags::FINISHED)
    }

    pub fn has_child(&self) -> bool {
        self.flags().contains(TransactionFlags::HAS_CHILD)
    }

    /// Initialize a read-only transaction
    fn init_read_txn(txn: &mut Transaction, env: &Environment) -> Result<()> {
        txn.txn_id = env.current_txn_id();
        env.register_reader()
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
        txn.next_page = next_page;
        txn.last_page = last_page;
        env.set_active_write_txn(true);
        Ok(())
    }

    fn write_meta_page(&self, meta_idx: u64) -> Result<()> {
        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        // Validate environment map
        let map = self.env.map.as_ref()
            .ok_or(Error::EnvNotInitialized)?;

        // Write meta page to disk
        let meta = MetaPage {
            magic: MAGIC,
            version: VERSION,
            flags: self.flags.load(Ordering::SeqCst),
            txn_id: self.txn_id,
            last_page: self.last_page,
            next_page: self.next_page,
            root_page: self.root_page,
        };

        // Calculate meta page offset
        let offset = meta_idx * self.env.page_size as u64;
        
        // Enhanced error handling for write operations
        if let Err(e) = map.write_at(&meta, offset) {
            return Err(Error::SyncFailed);
        }

        if !self.flags().intersects(TransactionFlags::NOSYNC | TransactionFlags::NOMETASYNC) {
            if let Err(e) = map.flush() {
                return Err(Error::SyncFailed);
            }
        }

        Ok(())
    }

    fn spill_pages(&mut self) -> Result<()> {
        // Validate transaction state
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        let mut need = self.dirty_pages.len() - self.env.max_dirty();
        
        if need > 0 {
            // Validate environment map
            let map = self.env.map.as_ref()
                .ok_or(Error::EnvNotInitialized)?;

            // Sort spill pages to maintain consistency
            self.spill_pages.sort_unstable();
            
            // Enhanced error handling for flush operations
            for page in &self.dirty_pages {
                // Skip pages marked as loose
                if self.loose_pages.contains(page) {
                    continue;
                }

                let offset = page * self.env.page_size as u64;
                if let Err(e) = map.flush_range(offset as usize, self.env.page_size) {
                    return Err(Error::SyncFailed);
                }
                
                // Add to spill pages list
                self.spill_pages.push(*page);
                need -= 1;
                if need == 0 {
                    break;
                }
            }

            // Mark transaction as having spilled pages
            self.add_flags(TransactionFlags::SPILLS);
        }
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
        if parent.dbs.len() > self.env.max_dbs() {
            return Err(Error::DbsFull);
        }

        // Copy parent's database state
        self.dbs = parent.dbs.clone();
        self.db_flags = parent.db_flags.clone();
        
        // Share read-only resources
        self.txn_id = parent.txn_id;
        self.root_page = parent.root_page;
        
        // Initialize separate dirty page tracking
        self.dirty_pages = Vec::new();
        self.spill_pages = Vec::new();

        Ok(())
    }

    fn register_reader(&self) -> Result<()> {
        if self.is_readonly() {
            if self.env.num_readers() >= self.env.max_readers() {
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

        if self.next_page >= self.last_page {
            return Err(Error::MapFull);
        }
        
        let page_no = self.next_page;
        self.next_page += 1;
        
        // Track as dirty
        self.dirty_pages.push(page_no);
        
        Ok(page_no)
    }
    
    fn free_page(&mut self, page_no: u64) {
        self.free_pages.push(page_no);
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
        if self.dirty_pages.len() + child.dirty_pages.len() > self.env.max_dirty() {
            return Err(Error::DirtyPagesExceeded);
        }

        // Merge dirty pages
        self.dirty_pages.extend(child.dirty_pages);
        
        // Merge spill pages
        if child.flags().contains(TransactionFlags::SPILLS) {
            self.spill_pages.extend(child.spill_pages);
            self.add_flags(TransactionFlags::SPILLS);
        }
        
        // Update database states
        self.dbs = child.dbs;
        self.db_flags = child.db_flags;
        
        // Update transaction state
        self.next_page = child.next_page;
        self.last_page = child.last_page;
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

        if page.number() >= self.last_page {
            return Err(Error::PageNotFound);
        }

        if page.flags().contains(PageFlags::DIRTY) {
            // Check if we can handle another dirty page
            if self.dirty_pages.len() >= self.env.max_dirty() {
                return Err(Error::DirtyPagesExceeded);
            }
            self.loose_pages.push(page.number());
        } else {
            self.free_pages.push(page.number());
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

        if self.is_readonly() && !db.flags().contains(DbFlags::RDONLY) {
            return Err(Error::TxnReadOnlyOp);
        }

        Ok(())
    }

    // Add validation for page operations
    fn validate_page_operation(&self, page_no: u64) -> Result<()> {
        if page_no == 0 || page_no >= self.last_page {
            return Err(Error::InvalidPageNumber);
        }

        if self.free_pages.contains(&page_no) {
            return Err(Error::PageAlreadyFreed);
        }

        Ok(())
    }

    // Enhanced transaction state transition
    fn transition_state(&mut self, new_flags: TransactionFlags) -> Result<()> {
        let current_flags = self.flags();
        
        // Validate state transition
        match (current_flags, new_flags) {
            (f, _) if f.contains(TransactionFlags::ERROR) => {
                return Err(Error::BadTxn)
            },
            (f, _) if f.contains(TransactionFlags::FINISHED) => {
                return Err(Error::TxnInvalid)
            },
            (f, new) if f.contains(TransactionFlags::RDONLY) && 
                       !new.contains(TransactionFlags::RDONLY) => {
                return Err(Error::TxnReadOnly)
            },
            _ => {}
        }

        self.set_flags(new_flags);
        Ok(())
    }

    // Add validation for cursor operations
    fn validate_cursor_operation(&self, cursor: &Cursor) -> Result<()> {
        if self.is_finished() {
            return Err(Error::TxnInvalid);
        }

        if !self.cursors.contains(cursor) {
            return Err(Error::CursorNotFound);
        }

        if cursor.is_closed() {
            return Err(Error::CursorClosed);
        }

        Ok(())
    }

    // Enhanced resource cleanup
    fn cleanup_resources(&mut self) -> Result<()> {
        // Close all cursors
        for cursor in &mut self.cursors {
            if let Err(e) = cursor.close() {
                return Err(Error::CursorCloseFailed);
            }
        }
        self.cursors.clear();

        // Free all pages
        for page in &self.dirty_pages {
            if let Err(e) = self.handle_loose_page(&Page::from_number(*page)) {
                return Err(Error::PageFreeFailed);
            }
        }
        
        Ok(())
    }

    // Add validation for transaction boundaries
    fn validate_transaction_limits(&self) -> Result<()> {
        if self.dirty_pages.len() >= self.env.max_dirty() {
            return Err(Error::TxnDirtyLimit);
        }

        if self.cursors.len() >= self.env.max_cursors() {
            return Err(Error::TxnCursorLimit);
        }

        if self.dbs.len() >= self.env.max_dbs() {
            return Err(Error::TxnDbLimit);
        }

        Ok(())
    }

    // Add validation for page consistency
    fn validate_page_consistency(&self, page: &Page) -> Result<()> {
        if page.checksum() != page.calculate_checksum() {
            return Err(Error::PageCorrupted);
        }

        if page.txn_id() > self.txn_id {
            return Err(Error::PageVersionMismatch);
        }

        if page.size() > self.env.page_size {
            return Err(Error::PageSizeExceeded);
        }

        Ok(())
    }

    // Enhanced error recovery
    fn recover_from_error(&mut self, error: &Error) -> Result<()> {
        // Set error flag
        self.add_flags(TransactionFlags::ERROR);

        match error {
            Error::MapFull | Error::DirtyPagesExceeded => {
                // Try to spill pages
                self.spill_pages()?;
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
        let mut to_remove = Vec::new();
        
        for (idx, cursor) in self.cursors.iter().enumerate() {
            if cursor.is_unused() {
                if let Err(e) = cursor.close() {
                    return Err(Error::CursorCleanupFailed);
                }
                to_remove.push(idx);
            }
        }

        // Remove closed cursors
        for idx in to_remove.iter().rev() {
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
            if let Err(e) = cursor.close() {
                return Err(Error::CursorCleanupFailed);
            }
        }
        self.cursors.clear();

        // Ensure all dirty pages are handled
        if !self.is_readonly() {
            for page in &self.dirty_pages {
                if let Err(e) = self.handle_loose_page(&Page::from_number(*page)) {
                    return Err(Error::PageCleanupFailed);
                }
            }
        }

        // Release reader slot if needed
        self.release_reader();

        Ok(())
    }

    // Add isolation level validation
    fn validate_isolation_level(&self, operation: &str) -> Result<()> {
        let current_level = self.flags().intersection(
            TransactionFlags::RDONLY | 
            TransactionFlags::WRITEMAP | 
            TransactionFlags::NOSYNC
        );

        match operation {
            "write" if current_level.contains(TransactionFlags::RDONLY) => {
                return Err(Error::TxnIsolationConflict)
            },
            "snapshot" if !current_level.contains(TransactionFlags::RDONLY) => {
                return Err(Error::TxnIsolationConflict)
            },
            _ => Ok(())
        }
    }

    // Add error context tracking
    fn track_error(&mut self, error: Error, context: &str) -> Error {
        self.add_flags(TransactionFlags::ERROR);
        
        // Log error context if available
        if let Some(logger) = self.env.logger() {
            logger.error(&format!("Transaction error: {} in {}", error, context));
        }

        error
    }

    // Add transaction retry mechanism
    fn retry_operation<F, T>(&mut self, retries: u32, op: F) -> Result<T> 
    where F: Fn(&mut Self) -> Result<T>
    {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < retries {
            match op(self) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    match e {
                        // Retry on specific errors
                        Error::TxnFull | 
                        Error::MapResized |
                        Error::PageCorrupted => {
                            attempts += 1;
                            last_error = Some(e);
                            self.recover_from_error(&e)?;
                            continue;
                        },
                        // Don't retry on other errors
                        _ => return Err(e)
                    }
                }
            }
        }

        Err(Error::TxnRetryExhausted(last_error.unwrap_or(Error::Unknown)))
    }

    // Add deadlock detection
    fn check_deadlock(&self) -> Result<()> {
        if let Some(parent) = self.parent {
            let mut current = parent;
            while let Some(p) = current.parent {
                if std::ptr::eq(p as *const _, self as *const _) {
                    return Err(Error::TxnDeadlock);
                }
                current = p;
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

        // Propagate error to parent transaction
        if let Some(parent) = self.parent {
            parent.add_flags(TransactionFlags::ERROR);
        }

        // Attempt recovery if possible
        match error {
            Error::TxnFull | 
            Error::MapResized |
            Error::PageCorrupted => self.recover_from_error(&error),
            _ => Err(error)
        }
    }
}

impl<'env> Drop for Transaction<'env> {
    fn drop(&mut self) {
        if let Err(e) = self.safe_cleanup() {
            // Log cleanup failure but can't return error from drop
            self.add_flags(TransactionFlags::ERROR);
        }
    }
}
