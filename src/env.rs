use std::os::raw::{c_int, c_uint, c_void};
use std::path::Path;
use std::ptr;
use std::ffi::{CString, c_char};
use std::sync::atomic::{AtomicU32, Ordering};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::io::{self, Write, Seek, SeekFrom};
use std::mem;
use std::os::unix::io::FromRawFd;

use crate::constants::{
    TransactionFlags,
    EnvFlags,
    MDB_MAGIC,
    MDB_VERSION,
    META_PAGES,
    PAGE_SIZE,
    CP_COMPACT,
    VERSION_MAJOR,
    VERSION_MINOR,
    VERSION_PATCH,
};
use crate::error::{Error, Result};
use crate::transaction::Transaction;
use crate::database::Database;
use crate::meta::{MetaHeader, ReaderTable, ReaderInfo, Stat};

#[derive(Debug, Clone)]
pub struct EnvInfo {
    pub mapaddr: *mut c_void,
    pub mapsize: usize,
    pub last_pgno: usize,
    pub last_txnid: usize,
    pub max_readers: u32,
    pub num_readers: u32,
}

#[derive(Debug, Clone)]
pub struct Stat {
    pub psize: u32,
    pub depth: u32,
    pub branch_pages: usize,
    pub leaf_pages: usize,
    pub overflow_pages: usize,
    pub entries: usize,
}

/// Database Environment
pub struct Environment {
    /// Main data file
    data_file: File,
    /// Lock file
    lock_file: File,
    /// Memory map of the data file
    map: Option<memmap2::MmapMut>,
    /// Environment flags
    flags: EnvFlags,
    /// Page size
    page_size: u32,
    /// Maximum number of readers
    max_readers: u32,
    /// Current number of readers
    num_readers: AtomicU32,
    /// Process ID
    pid: u32,
    /// Path to database files
    path: Box<Path>,
    /// Maximum pages in map
    max_pages: u64,
    /// Size of data memory map
    map_size: usize,
    /// Maximum number of named databases
    max_dbs: u32,
    /// User-provided context
    userctx: *mut c_void,
}

/// Safe wrapper for environment operations
impl Environment {
    /// Create a new environment
    pub fn new() -> Result<Self> {
        Ok(Environment {
            data_file: File::new(),
            lock_file: File::new(),
            map: None,
            flags: EnvFlags::empty(),
            page_size: PAGE_SIZE as u32,
            max_readers: 126,
            num_readers: AtomicU32::new(0),
            pid: std::process::id(),
            path: Path::new("").into(),
            max_pages: 0,
            map_size: 0,
            max_dbs: 0,
            userctx: std::ptr::null_mut(),
        })
    }

    /// Open the environment at the specified path
    pub fn open(&mut self, path: &Path, flags: EnvFlags, mode: u32) -> Result<()> {
        // Validate environment state
        if !self.path.as_os_str().is_empty() {
            return Err(Error::EnvAlreadyOpen);
        }

        // Validate inputs
        if !path.exists() {
            return Err(Error::EnvInvalidPath);
        }

        // Create/open the data file with better error handling
        self.data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(mode)
            .open(path.join("data.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Create/open the lock file with better error handling
        self.lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(mode)
            .open(path.join("lock.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Store the path and flags
        self.path = path.into();
        self.flags = flags;

        // Initialize the memory map
        self.remap().map_err(|_| Error::MapFailed)?;

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
        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path.join("data.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Start a read transaction to ensure consistency
        let txn = self.begin_txn()?;

        // Get the current size
        let size = self.data_file.metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;
        
        // Get the memory map
        let src_map = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)?;

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write the rest of the data
        let data_size = size - meta_size;
        if data_size > 0 {
            dst_file.write_all(&src_map[meta_size..size])
                .map_err(|_| Error::SyncFailed)?;
        }

        // Sync the file to disk
        dst_file.sync_all()
            .map_err(|_| Error::SyncFailed)?;

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
        let txn = self.begin_txn()?;

        // Get the current size
        let size = self.data_file.metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;
        
        // Get the memory map
        let src_map = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)?;

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write the rest of the data in chunks to avoid large allocations
        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        let mut offset = meta_size;
        while offset < size {
            let chunk_size = std::cmp::min(CHUNK_SIZE, size - offset);
            dst_file.write_all(&src_map[offset..offset + chunk_size])
                .map_err(|_| Error::SyncFailed)?;
            offset += chunk_size;
        }

        // Sync the file to disk
        dst_file.sync_all()
            .map_err(|_| Error::SyncFailed)?;

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
        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path.join("data.mdb"))
            .map_err(|_| Error::EnvInvalidPath)?;

        // Start a read transaction to ensure consistency
        let txn = self.begin_txn()?;

        // Get the memory map
        let src_map = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)?;

        // Create a cursor to walk through the free list
        let free_db = Database::open(&txn, None, 0)
            .map_err(|_| Error::DbOperationFailed)?;
        let mut free_cursor = free_db.cursor(&txn)
            .map_err(|_| Error::CursorNotFound)?;

        // Collect free pages
        let mut free_pages = Vec::new();
        while let Some((key, data)) = free_cursor.next()
            .map_err(|_| Error::CursorNotFound)? {
            let pgno = u64::from_le_bytes(key.try_into()
                .map_err(|_| Error::PageCorrupted)?);
            let count = u64::from_le_bytes(data.try_into()
                .map_err(|_| Error::PageCorrupted)?);
            free_pages.push((pgno, count));
        }

        // Sort free pages
        free_pages.sort_unstable_by_key(|&(pgno, _)| pgno);

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])
            .map_err(|_| Error::SyncFailed)?;

        // Write data pages, skipping free pages
        let mut write_offset = meta_size;
        let mut read_offset = meta_size;
        let size = self.data_file.metadata()
            .map_err(|_| Error::EnvInvalidFd)?
            .len() as usize;

        while read_offset < size {
            // Check if current page is in free list
            let current_pgno = (read_offset / self.page_size as usize) as u64;
            if let Some(pos) = free_pages.binary_search_by_key(&current_pgno, |&(pgno, _)| pgno).ok() {
                // Skip free pages
                let (_, count) = free_pages[pos];
                read_offset += self.page_size as usize * count as usize;
                continue;
            }

            // Write non-free page
            let chunk_size = self.page_size as usize;
            dst_file.write_all(&src_map[read_offset..read_offset + chunk_size])
                .map_err(|_| Error::SyncFailed)?;
            
            // Update page number in the written page
            let new_pgno = (write_offset / self.page_size as usize) as u64;
            dst_file.seek(SeekFrom::Start(write_offset as u64))
                .map_err(|_| Error::SyncFailed)?;
            dst_file.write_all(&new_pgno.to_le_bytes())
                .map_err(|_| Error::SyncFailed)?;

            write_offset += chunk_size;
            read_offset += chunk_size;
        }

        // Update meta page with new information
        let mut meta = MetaHeader {
            magic: MDB_MAGIC,
            version: MDB_VERSION,
            format_id: 0,
            page_size: self.page_size,
            last_page: (write_offset / self.page_size as usize) as u64 - 1,
            last_txn_id: txn.id(),
            root_page: 0,
            free_pages: 0,
        };

        dst_file.seek(SeekFrom::Start(0))
            .map_err(|_| Error::SyncFailed)?;
        dst_file.write_all(unsafe {
            std::slice::from_raw_parts(
                &meta as *const _ as *const u8,
                std::mem::size_of::<MetaHeader>()
            )
        }).map_err(|_| Error::SyncFailed)?;

        // Sync the file to disk
        dst_file.sync_all()
            .map_err(|_| Error::SyncFailed)?;

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

        let map = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)?;

        // Handle write-mapped case
        if self.flags.contains(EnvFlags::WRITEMAP) {
            if force || !self.flags.contains(EnvFlags::MAPASYNC) {
                map.flush()
                    .map_err(|_| Error::SyncFailed)?;
            }
            return Ok(());
        }

        // Handle standard case
        if force || !self.flags.intersects(EnvFlags::NOSYNC | EnvFlags::MAPASYNC) {
            self.data_file.sync_all()
                .map_err(|_| Error::SyncFailed)?;
        } else {
            self.data_file.sync_data()
                .map_err(|_| Error::SyncFailed)?;
        }

        Ok(())
    }

    /// Close the environment and release the memory map.
    /// 
    /// Only a single thread may call this function. All transactions, databases,
    /// and cursors must already be closed before calling this function.
    pub fn close(&mut self) {
        // Sync memory map if it exists
        if let Some(map) = self.map.take() {
            let _ = map.flush();
        }

        // Close files - they will be closed automatically when dropped,
        // but we do it explicitly here to match LMDB behavior
        let _ = self.data_file.sync_all();
        drop(std::mem::replace(&mut self.data_file, File::new()));
        drop(std::mem::replace(&mut self.lock_file, File::new()));

        // Reset state
        self.flags = EnvFlags::empty();
        self.map_size = 0;
        self.max_pages = 0;
        self.num_readers.store(0, Ordering::Relaxed);
        self.path = Path::new("").into();
        self.userctx = std::ptr::null_mut();
    }

    /// Set environment flags.
    /// 
    /// This may be used to set some flags in addition to those from open(),
    /// or to unset these flags. Only certain flags may be changed after 
    /// the environment is opened.
    pub fn set_flags(&mut self, flags: EnvFlags, onoff: bool) -> Result<()> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Define which flags can be changed after opening
        const CHANGEABLE: EnvFlags = EnvFlags::NOSYNC | 
                                   EnvFlags::NOMETASYNC | 
                                   EnvFlags::MAPASYNC | 
                                   EnvFlags::NOMEMINIT;
        
        // Validate flags
        if !flags.intersects(CHANGEABLE) {
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
        Ok(self.flags)
    }

    /// Get the path that was used in open()
    /// 
    /// Returns the path that was used in the Environment::open() call.
    pub fn get_path(&self) -> Result<&Path> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
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
        if !self.path.as_os_str().is_empty() {
            return Err(Error::EnvAlreadyOpen);
        }

        self.map_size = size;
        self.remap()?;
        Ok(())
    }

    /// Set the maximum number of threads/reader slots
    pub fn set_max_readers(&mut self, readers: u32) -> Result<()> {
        // Validate input
        if readers == 0 {
            return Err(Error::EnvInvalidMaxReaders);
        }
        
        // Validate environment state
        if !self.path.as_os_str().is_empty() {
            return Err(Error::EnvAlreadyOpen);
        }

        self.max_readers = readers;
        Ok(())
    }

    /// Get the maximum number of threads/reader slots
    /// 
    /// Returns the maximum number of reader slots for the environment.
    /// This defines the number of concurrent read transactions.
    pub fn get_max_readers(&self) -> Result<u32> {
        Ok(self.max_readers)
    }

    /// Set the maximum number of named databases
    /// 
    /// This function is only needed if multiple databases will be used in the
    /// environment. This function must be called before opening the environment.
    pub fn set_max_dbs(&mut self, dbs: u32) -> Result<()> {
        // Validate environment state
        if !self.path.as_os_str().is_empty() {
            return Err(Error::EnvAlreadyOpen);
        }

        // Validate input
        if dbs == 0 {
            return Err(Error::EnvInvalidConfig);
        }

        self.max_dbs = dbs;
        Ok(())
    }

    /// Get the maximum number of named databases
    /// 
    /// Returns the maximum number of named databases that can be opened
    /// in the environment.
    pub fn get_max_dbs(&self) -> Result<u32> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }
        Ok(self.max_dbs)
    }

    /// Get the maximum size of keys we can write
    /// 
    /// Returns the maximum size of a key that can be written to the database.
    /// This is platform dependent but is typically around 511 bytes.
    pub fn get_max_keysize(&self) -> Result<u32> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Default max key size is 511 bytes unless in development mode
        #[cfg(debug_assertions)]
        return Ok(0); // Development mode - computed max

        #[cfg(not(debug_assertions))]
        return Ok(511); // Production mode - fixed size
    }

    /// Set application information associated with the environment
    /// 
    /// This can be used to store a pointer to application-specific data
    /// that can be retrieved later with get_userctx().
    pub fn set_userctx(&mut self, ctx: *mut c_void) -> Result<()> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Validate input
        if ctx.is_null() {
            return Err(Error::EnvInvalidConfig);
        }

        self.userctx = ctx;
        Ok(())
    }

    /// Get the application information associated with the environment
    /// 
    /// Returns the pointer that was previously stored with set_userctx().
    pub fn get_userctx(&self) -> Result<*mut c_void> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        // Check if userctx is set
        if self.userctx.is_null() {
            return Err(Error::EnvInvalidConfig);
        }

        Ok(self.userctx)
    }

    /// Begin a new transaction
    pub fn begin_txn(&self) -> Result<Transaction> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }
        if self.path.as_os_str().is_empty() {
            return Err(Error::EnvInvalidPath);
        }

        Transaction::new(self)
    }

    /// Begin a read-only transaction
    pub fn begin_ro_txn(&self) -> Result<Transaction> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }
        if self.path.as_os_str().is_empty() {
            return Err(Error::EnvInvalidPath);
        }

        Transaction::new(self, None, TransactionFlags::RDONLY)
    }

    pub fn reader_list(&self) -> Result<Vec<ReaderInfo>> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
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
        if reader_table.mr_numreaders > self.max_readers {
            return Err(Error::Corrupted);
        }

        let mut readers = Vec::new();

        // Iterate through reader slots
        for i in 0..reader_table.mr_numreaders {
            let reader = &reader_table.mr_readers[i as usize];
            
            // Only include active readers
            if reader.mr_pid != 0 {
                readers.push(ReaderInfo {
                    pid: reader.mr_pid,
                    txnid: reader.mr_txnid,
                    thread: reader.mr_tid,
                });
            }
        }

        Ok(readers)
    }

    /// Get LMDB version
    pub fn version() -> (i32, i32, i32) {
        // Return version numbers from constants
        (VERSION_MAJOR as i32, VERSION_MINOR as i32, VERSION_PATCH as i32)
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
        if self.path.as_os_str().is_empty() {
            return Err(Error::EnvInvalidPath);
        }

        // Get meta page to read stats
        let meta = match &self.map {
            Some(map) => unsafe {
                &*(map.as_ptr() as *const MetaHeader)
            },
            None => return Err(Error::EnvNotInitialized),
        };

        Ok(Stat {
            psize: self.page_size,
            depth: meta.root_page,
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
        if self.path.as_os_str().is_empty() {
            return Err(Error::EnvInvalidPath);
        }

        // Get meta page to read info
        let meta = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)
            .map(|map| unsafe {
                &*(map.as_ptr() as *const MetaHeader)
            })?;

        Ok(EnvInfo {
            mapaddr: self.map.as_ref().map_or(std::ptr::null_mut(), |m| m.as_ptr() as *mut _),
            mapsize: self.map_size,
            last_pgno: meta.last_page as usize,
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
        if self.map_size == 0 {
            return Err(Error::EnvInvalidMapSize);
        }

        // Get file size
        let size = self.data_file.metadata()
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
        self.map_size = map_size;
        self.max_pages = (map_size / PAGE_SIZE) as u64;

        Ok(())
    }

    /// Register a reader for the current transaction
    pub(crate) fn register_reader(&self, txn_id: u64) -> Result<()> {
        // Validate reader count
        let current_readers = self.num_readers.load(Ordering::Relaxed);
        if current_readers >= self.max_readers {
            return Err(Error::ReadersFull);
        }
        
        self.num_readers.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Release a reader slot
    pub(crate) fn release_reader(&self, txn_id: u64) {
        if self.num_readers.load(Ordering::Relaxed) > 0 {
            self.num_readers.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get current transaction ID for read-only transactions
    pub(crate) fn current_txn_id(&self) -> Result<u64> {
        // Validate environment state
        if self.map.is_none() {
            return Err(Error::EnvNotInitialized);
        }

        let meta = self.map.as_ref()
            .ok_or(Error::EnvNotInitialized)
            .map(|map| unsafe {
                let meta_idx = self.meta_page & 1;
                let offset = meta_idx * self.page_size as u64;
                &*(map.as_ptr().add(offset as usize) as *const MetaPage)
            })?;

        Ok(meta.txn_id)
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        self.close();
    }
}

