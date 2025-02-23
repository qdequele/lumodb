use lazy_static::lazy_static;
use memmap2::{MmapMut, MmapOptions};
use std::any::Any;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::raw::c_void;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::constants::{
    DbFlags, EnvFlags, TransactionFlags, DEFAULT_MAX_CURSORS, DEFAULT_MAX_DBS, DEFAULT_MAX_DIRTY,
    DEFAULT_MAX_KEY_SIZE, DEFAULT_MAX_READERS, DEFAULT_PAGE_SIZE, DEFAULT_READER_CHECK_SECS,
    MDB_MAGIC, META_PAGES, PAGE_SIZE, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH,
};
use crate::cursor::Page;
use crate::database::Database;
use crate::error::{Error, Result};
use crate::meta::{MetaHeader, MetaPage, ReaderInfo, ReaderStatus, ReaderTable, Stat};
use crate::transaction::Transaction;

/// Copy operation flags
pub const CP_COMPACT: u32 = 0x01;

/// Database version
pub const MDB_VERSION: u32 = VERSION_MAJOR << 24 | VERSION_MINOR << 16 | VERSION_PATCH;

/// Meta page size
pub const META_SIZE: usize = std::mem::size_of::<MetaPage>();

lazy_static! {
    static ref CHANGEABLE: EnvFlags =
        EnvFlags::NOSYNC | EnvFlags::NOMETASYNC | EnvFlags::MAPASYNC | EnvFlags::NOMEMINIT;
}

#[derive(Debug, Clone)]
pub struct EnvInfo {
    pub mapaddr: *mut c_void,
    pub mapsize: usize,
    pub last_pgno: usize,
    pub last_txnid: usize,
    pub max_readers: u32,
    pub num_readers: u32,
}

/// LMDB Environment matching C implementation
#[derive(Debug)]
pub struct Environment<'env> {
    /// Path to the environment directory
    pub(crate) path: PathBuf,
    /// Environment flags
    pub(crate) flags: EnvFlags,
    /// Page size
    pub(crate) page_size: usize,
    /// Maximum key size
    pub(crate) max_key_size: usize,
    /// Maximum number of databases
    pub(crate) max_dbs: u32,
    /// Maximum number of readers
    pub(crate) max_readers: u32,
    /// Maximum number of cursors
    pub(crate) max_cursors: u32,
    /// Maximum number of dirty pages
    pub(crate) max_dirty: u32,
    /// Memory map
    pub(crate) map: Option<MmapMut>,
    /// Data file
    pub(crate) data_file: File,
    /// Lock file
    pub(crate) lock_file: Option<File>,
    /// User context
    pub(crate) userctx: Option<Box<dyn Any + Send + Sync>>,
    /// Number of readers
    pub(crate) num_readers: AtomicU32,
    /// Has active write transaction
    pub(crate) has_write_txn: AtomicBool,
    /// Environment is initialized
    pub(crate) initialized: AtomicBool,
    /// Meta pages
    pub(crate) meta_pages: [Option<Box<MetaPage>>; 2],
    /// Free page list
    pub(crate) free_pages: Vec<u64>,
    /// Dirty page list
    pub(crate) dirty_pages: Vec<u64>,
    /// Current meta page index
    pub(crate) meta_index: usize,
    /// Current transaction ID
    pub(crate) txn_id: u64,
    /// Reader check delay
    pub(crate) reader_check_delay: Duration,
    /// Meta page
    pub(crate) meta: Option<MetaHeader>,
    /// Readers
    pub(crate) readers: Vec<ReaderInfo>,
    /// Transactions
    pub(crate) txns: Vec<Transaction<'env>>,
}

/// Safe wrapper for environment operations
impl Environment<'_> {
    /// Create a new environment
    pub fn new() -> Result<Self> {
        Ok(Environment {
            path: PathBuf::new(),
            flags: EnvFlags::empty(),
            page_size: DEFAULT_PAGE_SIZE,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_dbs: DEFAULT_MAX_DBS,
            max_readers: DEFAULT_MAX_READERS,
            max_cursors: DEFAULT_MAX_CURSORS,
            max_dirty: DEFAULT_MAX_DIRTY,
            map: None,
            data_file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/dev/null")?, // Temporary file until real path is set
            lock_file: None,
            userctx: None,
            num_readers: AtomicU32::new(0),
            has_write_txn: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            meta_pages: [None, None],
            free_pages: Vec::new(),
            dirty_pages: Vec::new(),
            meta_index: 0,
            txn_id: 0,
            reader_check_delay: Duration::from_secs(DEFAULT_READER_CHECK_SECS),
            meta: None,
            readers: Vec::new(),
            txns: Vec::new(),
        })
    }

    /// Open the environment
    pub fn open<P: AsRef<Path>>(mut self, path: P, flags: EnvFlags) -> Result<Self> {
        if self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvMapFailed);
        }

        self.path = path.as_ref().to_path_buf();
        self.flags = flags;

        // Open data file
        let mut options = OpenOptions::new();
        options.read(true).write(!flags.contains(EnvFlags::RDONLY));
        if flags.contains(EnvFlags::FIXEDMAP) {
            options.create(true);
        }

        let data_file = options.open(&self.path)?;
        let file_size = data_file.metadata()?.len();

        // Create memory map - always use MmapMut since we need to modify meta pages
        let map = unsafe {
            MmapOptions::new()
                .len(file_size as usize)
                .map_mut(&data_file)?
        };

        self.data_file = data_file;
        self.map = Some(map);

        // Initialize meta pages if new database
        if file_size == 0 {
            self.init_meta_pages()?;
        } else {
            // Read existing meta pages
            self.read_meta_pages()?;
        }

        self.initialized.store(true, Ordering::SeqCst);
        let txn = Transaction::new(&self, flags.contains(EnvFlags::RDONLY))?;
        self.txns.push(txn);
        let txn = self.txns.last().unwrap();

        Ok(self)
    }

    /// Initialize meta pages
    fn init_meta_pages(&mut self) -> Result<()> {
        let meta = MetaHeader {
            magic: MDB_MAGIC,
            version: VERSION_MAJOR << 24 | VERSION_MINOR << 16 | VERSION_PATCH,
            format_id: 0,
            page_size: self.page_size as u32,
            address: 0,
            mapsize: self.page_size,
            dbs: Vec::new(),
            last_pgno: 0,
            last_txn_id: 0,
            root_page: 0,
            free_pages: 0,
            txnid: 0,
        };

        let meta_size = std::mem::size_of::<MetaHeader>();
        let map = self.map.as_ref().ok_or(Error::EnvNotInitialized)?;

        unsafe {
            let ptr = map.as_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(&meta as *const MetaHeader as *const u8, ptr, meta_size);
            std::ptr::copy_nonoverlapping(
                &meta as *const MetaHeader as *const u8,
                ptr.add(self.page_size),
                meta_size,
            );
        }

        self.meta = Some(meta);
        Ok(())
    }

    /// Read meta pages from the environment
    fn read_meta_pages(&mut self) -> Result<()> {
        let map = self.map.as_ref().ok_or(Error::EnvNotInitialized)?;

        if map.len() < META_SIZE {
            return Err(Error::Invalid);
        }

        unsafe {
            let ptr = map.as_ptr() as *const MetaHeader;
            let meta = ptr.read();

            // Validate magic number
            if meta.magic != MDB_MAGIC {
                return Err(Error::Invalid);
            }

            // Validate version
            let version = VERSION_MAJOR << 24 | VERSION_MINOR << 16 | VERSION_PATCH;
            if meta.version != version {
                return Err(Error::VersionMismatch);
            }

            self.meta = Some(meta);
        }

        Ok(())
    }

    /// Copy environment to the specified path
    ///
    /// This function may be used to make a backup of an existing environment.
    /// No lockfile is created, since it gets recreated at need.
    pub fn copy(&self, path: &Path) -> Result<()> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Validate destination path
        if path.exists() && !path.is_dir() {
            return Err(Error::EnvInvalidPath);
        }

        // Open the destination file with write access
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path.join("data.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Start a read transaction to ensure consistency
        let _txn = self.begin_ro_txn()?;

        // Get the current size
        let size = self
            .data_file
            .metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;

        // Validate size:
        if size < META_SIZE {
            return Err(Error::Corrupted);
        }

        // Get the memory map
        let src_map = self.map.as_ref().ok_or(Error::EnvNotInitialized)?;

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file
            .write_all(&src_map[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write the rest of the data in chunks to avoid large allocations
        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        let mut offset = meta_size;
        while offset < size {
            let chunk_size = std::cmp::min(CHUNK_SIZE, size - offset);
            dst_file
                .write_all(&src_map[offset..offset + chunk_size])
                .map_err(|_| Error::SyncFailed)?;
            offset += chunk_size;
        }

        // Sync the file to disk
        dst_file.sync_all().map_err(|_| Error::SyncFailed)?;

        // Transaction will be aborted on drop
        Ok(())
    }

    /// Copy environment to the specified file descriptor
    ///
    /// This function may be used to make a backup of an existing environment.
    /// Note that the file descriptor must be opened with write permission.
    pub fn copy_fd(&self, fd: i32) -> Result<()> {
        // Validate input
        if fd < 0 {
            return Err(Error::EnvInvalidFd);
        }

        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Convert raw fd to File for safe handling
        let mut dst_file = unsafe { File::from_raw_fd(fd) };

        // Start a read transaction to ensure consistency
        let _txn = self.begin_ro_txn()?;

        // Get the current size
        let size = self
            .data_file
            .metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;

        // Get the memory map
        let src_map = self.map.as_ref().ok_or(Error::EnvNotInitialized)?;

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file
            .write_all(&src_map[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write the rest of the data in chunks to avoid large allocations
        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        let mut offset = meta_size;
        while offset < size {
            let chunk_size = std::cmp::min(CHUNK_SIZE, size - offset);
            dst_file
                .write_all(&src_map[offset..offset + chunk_size])
                .map_err(|_| Error::SyncFailed)?;
            offset += chunk_size;
        }

        // Sync the file to disk
        dst_file.sync_all().map_err(|_| Error::SyncFailed)?;

        // Don't close the file descriptor since we don't own it
        std::mem::forget(dst_file);

        // Transaction will be aborted on drop
        Ok(())
    }

    /// Copy environment with compaction option
    pub fn copy2(&self, path: &Path, flags: u32) -> Result<()> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        if flags & CP_COMPACT != 0 {
            self.copy_compact(path)
        } else {
            self.copy(path)
        }
    }

    /// Internal method to perform compacting copy
    fn copy_compact(&self, path: &Path) -> Result<()> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Validate destination path
        if path.exists() && !path.is_dir() {
            return Err(Error::EnvInvalidPath);
        }

        // Open the destination file with write access
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path.join("data.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Start a read transaction to ensure consistency
        let mut txn = self.begin_ro_txn()?;

        // Collect free pages in a separate scope
        let free_pages = {
            let mut pages = Vec::new();
            let free_db = Database::open(&mut txn, None, DbFlags::empty())?;
            let mut cursor = free_db.cursor()?;

            while let Some((key, data)) = cursor.next()? {
                let key_bytes: [u8; 8] = key.try_into().map_err(|_| Error::BadValSize)?;
                let data_bytes: [u8; 8] = data.try_into().map_err(|_| Error::BadValSize)?;

                let pgno = u64::from_le_bytes(key_bytes);
                let count = u64::from_le_bytes(data_bytes);
                pages.push((pgno, count));
            }

            // Database and cursor are dropped here
            pages
        };

        // Sort free pages
        free_pages.sort_unstable_by_key(|&(pgno, _)| pgno);

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file
            .write_all(&self.map.as_ref().ok_or(Error::EnvNotInitialized)?[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write data pages, skipping free pages
        let mut write_offset = meta_size;
        let mut read_offset = meta_size;
        let size = self
            .data_file
            .metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;

        while read_offset < size {
            // Check if current page is in free list
            let current_pgno = (read_offset / self.page_size as usize) as u64;
            if let Some(pos) = free_pages
                .binary_search_by_key(&current_pgno, |&(pgno, _)| pgno)
                .ok()
            {
                // Skip free pages
                let (_, count) = free_pages[pos];
                read_offset += self.page_size as usize * count as usize;
                continue;
            }

            // Write non-free page
            let chunk_size = self.page_size as usize;
            dst_file
                .write_all(
                    &self.map.as_ref().ok_or(Error::EnvNotInitialized)?
                        [read_offset..read_offset + chunk_size],
                )
                .map_err(|_| Error::SyncFailed)?;

            // Update page number in the written page
            let new_pgno = (write_offset / self.page_size as usize) as u64;
            dst_file
                .seek(SeekFrom::Start(write_offset as u64))
                .map_err(|_| Error::SyncFailed)?;
            dst_file
                .write_all(&new_pgno.to_le_bytes())
                .map_err(|_| Error::SyncFailed)?;

            write_offset += chunk_size;
            read_offset += chunk_size;
        }

        // Update meta page with new information
        let mut meta = MetaHeader {
            magic: MDB_MAGIC,
            version: MDB_VERSION,
            format_id: 0,
            page_size: self.page_size as u32,
            address: 0,
            mapsize: write_offset, // Use the final write offset as the map size
            dbs: Vec::new(),
            last_pgno: (write_offset / self.page_size as usize) as u64 - 1,
            last_txn_id: txn.id(),
            root_page: 0,
            free_pages: 0,
            txnid: txn.id(),
        };

        dst_file
            .seek(SeekFrom::Start(0))
            .map_err(|_| Error::SyncFailed)?;
        dst_file
            .write_all(unsafe {
                std::slice::from_raw_parts(
                    &meta as *const _ as *const u8,
                    std::mem::size_of::<MetaHeader>(),
                )
            })
            .map_err(|_| Error::SyncFailed)?;

        // Sync the file to disk
        dst_file.sync_all().map_err(|_| Error::SyncFailed)?;

        Ok(())
    }

    /// Flush the data buffers to disk
    ///
    /// Data is always written to disk when transactions commit, but the operating system
    /// may keep it buffered. This function ensures the OS actually writes the data to
    /// physical storage. If force is true, the sync will be synchronous (immediate).
    ///
    /// This function is not valid if the environment was opened with RDONLY.
    pub fn sync(&self, force: bool) -> Result<()> {
        // Validate environment state and flags
        if self.flags.contains(EnvFlags::RDONLY) {
            return Err(Error::EnvReadOnly);
        }

        let map = self.map.as_ref().ok_or(Error::EnvNotInitialized)?;

        // Handle write-mapped case
        if self.flags.contains(EnvFlags::WRITEMAP) {
            if force || !self.flags.contains(EnvFlags::MAPASYNC) {
                map.flush().map_err(|_| Error::SyncFailed)?;
            }
            return Ok(());
        }

        // Handle standard case
        if force || !self.flags.intersects(EnvFlags::NOSYNC | EnvFlags::MAPASYNC) {
            self.data_file.sync_all().map_err(|_| Error::SyncFailed)?;
        } else {
            self.data_file.sync_data().map_err(|_| Error::SyncFailed)?;
        }

        Ok(())
    }

    /// Close the environment and release the memory map.
    ///
    /// Only a single thread may call this function. All transactions, databases,
    /// and cursors must already be closed before calling this function.
    pub fn close(&mut self) {
        self.map = None;
        self.data_file = unsafe { File::from_raw_fd(-1) };
        self.lock_file = None;
        self.path.clear();
        self.flags = EnvFlags::empty();
        self.initialized.store(false, Ordering::SeqCst);
        self.has_write_txn.store(false, Ordering::SeqCst);
        self.num_readers.store(0, Ordering::SeqCst);
    }

    /// Set environment flags.
    ///
    /// This may be used to set some flags in addition to those from open(),
    /// or to unset these flags. Only certain flags may be changed after
    /// the environment is opened.
    pub fn set_flags(&mut self, flags: EnvFlags, onoff: bool) -> Result<()> {
        // Validate environment state
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvNotInitialized);
        }

        // Validate flags
        if !flags.intersects(*CHANGEABLE) {
            return Err(Error::EnvFlagsImmutable);
        }

        // Update flags
        if onoff {
            self.flags |= flags;
        } else {
            self.flags &= !flags;
        }

        Ok(())
    }

    /// Get environment flags.
    ///
    /// Returns the flags currently set in the environment.
    pub fn get_flags(&self) -> Result<EnvFlags> {
        Ok(self.flags.clone())
    }

    /// Get the path that was used in open()
    ///
    /// Returns the path that was used in the Environment::open() call.
    pub fn get_path(&self) -> Result<&Path> {
        // Check if environment is initialized
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvNotInitialized);
        }
        Ok(&self.path)
    }

    /// Get the file descriptor for the environment
    ///
    /// Returns the file descriptor for the main data file.
    /// This can be used to close the file descriptor before exec*() calls.
    pub fn get_fd(&self) -> Result<i32> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Get the raw fd from the data file
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            Ok(self.data_file.as_raw_fd())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            Ok(self.data_file.as_raw_handle() as i32)
        }
    }

    /// Set the size of the memory map
    pub fn set_map_size(&mut self, size: usize) -> Result<()> {
        // Validate input
        if size == 0 {
            return Err(Error::EnvInvalidMapSize);
        }

        // Validate environment state
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvNotInitialized);
        }

        self.max_key_size = size;
        self.remap()?;
        Ok(())
    }

    /// Set the maximum number of threads/reader slots
    pub fn set_max_readers(&mut self, readers: u32) -> Result<()> {
        if self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvMapFailed);
        }
        self.max_readers = readers;
        Ok(())
    }

    /// Get the maximum number of threads/reader slots
    ///
    /// Returns the maximum number of reader slots for the environment.
    /// This defines the number of concurrent read transactions.
    pub fn get_max_readers(&self) -> u32 {
        self.max_readers
    }

    /// Set the maximum number of named databases
    ///
    /// This function is only needed if multiple databases will be used in the
    /// environment. This function must be called before opening the environment.
    pub fn set_max_dbs(&mut self, dbs: u32) -> Result<()> {
        if self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvMapFailed);
        }
        self.max_dbs = dbs;
        Ok(())
    }

    /// Get the maximum number of named databases
    ///
    /// Returns the maximum number of named databases that can be opened
    /// in the environment.
    pub fn get_max_dbs(&self) -> u32 {
        self.max_dbs
    }

    /// Get the maximum size of keys we can write
    ///
    /// Returns the maximum size of a key that can be written to the database.
    /// This is platform dependent but is typically around 511 bytes.
    pub fn get_max_key_size(&self) -> u32 {
        self.max_key_size as u32
    }

    /// Set application information associated with the environment
    ///
    /// This can be used to store a pointer to application-specific data
    /// that can be retrieved later with get_userctx().
    pub fn set_userctx<T: Any + Send + Sync>(&mut self, ctx: T) {
        self.userctx = Some(Box::new(ctx));
    }

    /// Get the application information associated with the environment
    ///
    /// Returns the pointer that was previously stored with set_userctx().
    pub fn get_userctx(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.userctx.as_deref()
    }

    /// Begin a new transaction
    pub fn begin_txn(&self) -> Result<Transaction> {
        Transaction::new(self, false)
    }

    /// Begin a new read-only transaction
    pub fn begin_ro_txn(&self) -> Result<Transaction> {
        Transaction::new(self, true)
    }

    pub fn reader_list(&self) -> Result<Vec<ReaderInfo>> {
        // Check if environment is initialized
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvNotInitialized);
        }

        // Get reader lock table from memory map
        let reader_table = match &self.map {
            Some(map) => unsafe {
                if map.len() < self.page_size {
                    return Err(Error::Corrupted);
                }
                let ptr = map.as_ptr().add(self.page_size);
                &*(ptr as *const ReaderTable)
            },
            None => return Err(Error::EnvNotInitialized),
        };

        // Validate reader count
        if reader_table.num_readers > self.max_readers {
            return Err(Error::Corrupted);
        }

        let mut readers = Vec::new();

        // Iterate through reader slots
        for i in 0..reader_table.num_readers {
            let reader = &reader_table.readers[i as usize];

            // Only include active readers
            if reader.pid != 0 {
                readers.push(ReaderInfo {
                    pid: reader.pid,
                    txn_id: reader.txn_id,
                    status: reader.status,
                });
            }
        }

        Ok(readers)
    }

    /// Get LMDB version
    pub fn version() -> (i32, i32, i32) {
        // Return version numbers from constants
        (
            VERSION_MAJOR as i32,
            VERSION_MINOR as i32,
            VERSION_PATCH as i32,
        )
    }

    /// Get LMDB version in String format
    pub fn version_string() -> String {
        format!("{}.{}.{}", VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
    }

    /// Get environment statistics
    ///
    /// Returns statistics about the LMDB environment.
    pub fn stat(&self) -> Result<Stat> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvInvalidPath);
        }

        // Get meta page to read stats
        let meta = match &self.map {
            Some(map) => unsafe { &*(map.as_ptr() as *const MetaHeader) },
            None => return Err(Error::EnvNotInitialized),
        };

        Ok(Stat {
            psize: self.page_size as u32,
            depth: meta.root_page as u32,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
        })
    }

    /// Get environment information
    ///
    /// Returns information about the LMDB environment.
    pub fn info(&self) -> Result<EnvInfo> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvInvalidPath);
        }

        // Get meta page to read info
        let meta = self
            .map
            .as_ref()
            .ok_or(Error::EnvNotInitialized)
            .map(|map| unsafe { &*(map.as_ptr() as *const MetaHeader) })?;

        Ok(EnvInfo {
            mapaddr: self
                .map
                .as_ref()
                .map_or(std::ptr::null_mut(), |m| m.as_ptr() as *mut _),
            mapsize: self.max_key_size,
            last_pgno: meta.last_pgno as usize,
            last_txnid: meta.last_txn_id as usize,
            max_readers: self.max_readers,
            num_readers: self.num_readers.load(Ordering::Relaxed),
        })
    }

    /// Check for stale readers
    pub fn reader_check(&self) -> Result<usize> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // This is a placeholder - actual implementation would check for
        // and clean up stale reader slots
        Ok(0)
    }

    // Internal helper methods
    fn remap(&mut self) -> Result<()> {
        // Validate map size
        if self.max_key_size == 0 {
            return Err(Error::EnvInvalidMapSize);
        }

        // Get file size
        let size = self
            .data_file
            .metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;

        // Round up to page size
        let map_size = if size == 0 {
            PAGE_SIZE
        } else {
            (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
        };

        // Create the memory map
        let map = unsafe {
            memmap2::MmapOptions::new()
                .len(map_size)
                .map_mut(&self.data_file)
                .map_err(|_| Error::MapFailed)?
        };

        self.map = Some(map);
        self.max_key_size = map_size;

        Ok(())
    }

    /// Register a reader
    pub(crate) fn register_reader(&self, txn_id: u64) -> Result<()> {
        if self.num_readers.load(Ordering::SeqCst) >= self.max_readers {
            return Err(Error::ReadersFull);
        }

        self.readers.push(ReaderInfo {
            txn_id,
            pid: std::process::id(),
            status: ReaderStatus::Active,
        });

        self.num_readers.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Release a reader slot
    pub(crate) fn release_reader(&self, _txn_id: u64) {
        self.num_readers.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get current transaction ID
    pub(crate) fn current_txn_id(&self) -> Result<u64> {
        if let Some(meta) = &self.meta {
            Ok(meta.txnid)
        } else {
            Err(Error::Invalid)
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    pub fn is_readonly(&self) -> bool {
        self.flags.contains(EnvFlags::RDONLY)
    }

    pub fn get_max_cursors(&self) -> u32 {
        self.max_cursors
    }

    pub fn get_max_dirty(&self) -> usize {
        self.max_dirty as usize
    }

    pub fn has_active_write_txn(&self) -> bool {
        self.has_write_txn.load(Ordering::SeqCst)
    }

    pub fn set_active_write_txn(&self, active: bool) {
        self.has_write_txn.store(active, Ordering::SeqCst)
    }

    pub fn next_page(&self) -> u64 {
        let meta = unsafe { &*(self.map.as_ref().unwrap().as_ptr() as *const MetaPage) };
        meta.header.root_page
    }

    pub fn last_page(&self) -> u64 {
        let meta = unsafe { &*(self.map.as_ref().unwrap().as_ptr() as *const MetaPage) };
        meta.header.last_pgno
    }

    pub fn next_txn_id(&self) -> u64 {
        let meta = unsafe { &*(self.map.as_ref().unwrap().as_ptr() as *const MetaPage) };
        meta.header.last_txn_id + 1
    }

    pub fn meta_page(&self) -> u64 {
        let meta = unsafe { &*(self.map.as_ref().unwrap().as_ptr() as *const MetaPage) };
        meta.header.root_page
    }

    pub(crate) fn get_page(&self, pgno: u64) -> Result<NonNull<Page>> {
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(Error::EnvNotInitialized);
        }

        // For now return a placeholder - actual implementation would read from mmap
        Err(Error::PageNotFound)
    }

    pub(crate) fn get_dirty_page(&self, pgno: u64) -> Result<NonNull<Page>> {
        // For now return a placeholder - actual implementation would read from dirty page cache
        Err(Error::PageNotFound)
    }
}

impl Drop for Environment<'_> {
    fn drop(&mut self) {
        self.close();
    }
}
