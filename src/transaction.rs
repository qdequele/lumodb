use std::os::raw::c_void;
use std::sync::atomic::Ordering;
use std::ptr::null_mut;
use std::ptr::NonNull;
use std::ptr::NonNull::new;
use std::ptr::NonNull::new_unchecked;


// Constants for transaction flags
const MDB_TXN_FINISHED: u32 = 0x01;
const MDB_TXN_ERROR: u32 = 0x02;
const MDB_TXN_DIRTY: u32 = 0x04;
const MDB_TXN_SPILLS: u32 = 0x08;
const MDB_TXN_HAS_CHILD: u32 = 0x10;


/// Database transaction
pub struct Transaction<'env> {
    /// Reference to environment
    env: &'env Environment,
    /// Transaction flags 
    flags: u32,
    /// Transaction ID
    id: u64,
    /// Parent transaction for nested txns
    parent: Option<Box<Transaction<'env>>>,
    /// Child transaction
    child: Option<Box<Transaction<'env>>>,
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
}

impl<'env> Transaction<'env> {
    /// Create a new transaction
    pub(crate) fn new(env: &'env Environment) -> Result<Self> {
        // Start a read transaction to ensure consistency
        let mut txn = Transaction {
            env,
            flags: 0,
            id: env.num_readers.fetch_add(1, Ordering::SeqCst),
            parent: None,
            child: None,
            cursors: Vec::new(),
            dbs: Vec::new(),
            db_flags: Vec::new(),
            free_pages: Vec::new(),
            dirty_pages: Vec::new(),
            spill_pages: Vec::new(),
            root_page: 0,
            next_page: 0,
            last_page: 0,
            userctx: std::ptr::null_mut(),
        };

        // Get meta page to read transaction info
        let meta = match &env.map {
            Some(map) => unsafe {
                &*(map.as_ptr() as *const MetaHeader)
            },
            None => return Err(Error::Invalid),
        };

        // Initialize transaction state from meta page
        txn.id = meta.last_txn_id + 1;
        txn.root_page = meta.root_page;
        txn.next_page = meta.last_page + 1;
        txn.last_page = meta.last_page;

        // Copy database info
        txn.dbs.extend_from_slice(&meta.dbs[..]);
        txn.db_flags.extend_from_slice(&meta.db_flags[..]);

        Ok(txn)
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        // Check if transaction is still valid
        if self.flags & MDB_TXN_FINISHED != 0 {
            return Err(Error::from(libc::EINVAL));
        }

        // Cannot commit nested transaction with unfinished child
        if self.child.is_some() {
            return Err(Error::from(libc::EINVAL));
        }

        // For a read-only transaction, just reset state
        if self.flags & RDONLY != 0 {
            self.flags |= MDB_TXN_FINISHED;
            return Ok(());
        }

        // Commit all dirty pages to disk
        if let Some(map) = &self.env.map {
            // Write dirty pages
            for page in &self.dirty_pages {
                let offset = page * self.env.page_size as u64;
                let data = &self.dirty_pages[..self.env.page_size];
                map.flush_range(offset as usize, self.env.page_size)?;
            }

            // Sync if needed
            if self.flags & (NOSYNC | MAPASYNC) == 0 {
                map.flush()?;
            }
        }

        // Update transaction ID
        self.env.num_readers.fetch_add(1, Ordering::SeqCst);

        // Clear state
        self.flags |= MDB_TXN_FINISHED;
        self.dirty_pages.clear();
        self.free_pages.clear();
        self.spill_pages.clear();

        Ok(())
    }

    /// Abort the transaction
    pub fn abort(mut self) {
        // Check if transaction is still valid
        if self.flags & MDB_TXN_FINISHED != 0 {
            return;
        }

        // Abort any child transaction first
        if let Some(child) = self.child.take() {
            child.abort();
        }

        // For read-only transaction, just reset state
        if self.flags & RDONLY != 0 {
            self.flags |= MDB_TXN_FINISHED;
            return;
        }

        // Clear all dirty pages
        self.dirty_pages.clear();
        self.free_pages.clear();
        self.spill_pages.clear();

        // Mark transaction as finished
        self.flags |= MDB_TXN_FINISHED;

        // Release reader slot
        if let Some(reader) = self.env.num_readers.get_mut() {
            *reader -= 1;
        }
    }

    /// Get transaction ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get reference to environment
    pub fn env(&self) -> &Environment {
        self.env
    }
}

impl<'env> Drop for Transaction<'env> {
    fn drop(&mut self) {
        // Abort transaction if not already committed
        if self.flags & MDB_TXN_FINISHED == 0 {
            self.abort();
        }
    }
}
