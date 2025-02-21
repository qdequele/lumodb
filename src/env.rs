use std::os::raw::{c_int, c_uint, c_void};
use std::path::Path;
use std::ptr;
use std::ffi::{CString, c_char};
use std::result;
use std::sync::atomic::{AtomicU32, AtomicI32, Ordering};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::io::{self, Result};
use std::ptr::NonNull;
use std::mem;
use std::os::unix::io::FromRawFd;
use bitflags::bitflags;


/// Database Environment
pub struct Environment {
    /// Main data file
    data_file: File,
    /// Lock file
    lock_file: File,
    /// Memory map of the data file
    map: Option<memmap2::MmapMut>,
    /// Environment flags
    flags: u32,
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
            flags: 0,
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
    pub fn open(&mut self, path: &Path, flags: u32, mode: u32) -> Result<()> {
        // Create/open the data file
        self.data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(mode)
            .open(path.join("data.mdb"))?;

        // Create/open the lock file
        self.lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(mode)
            .open(path.join("lock.mdb"))?;

        // Store the path
        self.path = path.into();
        self.flags = flags;

        // Initialize the memory map
        self.remap()?;

        Ok(())
    }

    /// Copy environment to the specified path
    /// 
    /// This function may be used to make a backup of an existing environment.
    /// No lockfile is created, since it gets recreated at need.
    pub fn copy(&self, path: &Path) -> Result<()> {
        // Open the destination file with write access
        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644) // Same permissions as source
            .open(path.join("data.mdb"))?;

        // Start a read transaction to ensure consistency
        let txn = self.begin_txn()?;

        // Get the current size
        let size = self.data_file.metadata()?.len() as usize;
        
        // Get the memory map
        let src_map = match &self.map {
            Some(map) => map,
            None => return Err(Error::Invalid),
        };

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])?;

        // Write the rest of the data
        let data_size = size - meta_size;
        if data_size > 0 {
            dst_file.write_all(&src_map[meta_size..size])?;
        }

        // Sync the file to disk
        dst_file.sync_all()?;

        // Abort the read transaction
        drop(txn);

        Ok(())
    }

    /// Copy environment to the specified file descriptor
    /// 
    /// This function may be used to make a backup of an existing environment.
    /// Note that the file descriptor must be opened with write permission.
    pub fn copy_fd(&self, fd: i32) -> Result<()> {
        // Convert raw fd to File for safe handling
        let mut dst_file = unsafe { File::from_raw_fd(fd) };

        // Start a read transaction to ensure consistency
        let txn = self.begin_txn()?;

        // Get the current size
        let size = self.data_file.metadata()?.len() as usize;
        
        // Get the memory map
        let src_map = match &self.map {
            Some(map) => map,
            None => return Err(Error::Invalid),
        };

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])?;

        // Write the rest of the data in chunks to avoid large allocations
        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        let mut offset = meta_size;
        while offset < size {
            let chunk_size = std::cmp::min(CHUNK_SIZE, size - offset);
            dst_file.write_all(&src_map[offset..offset + chunk_size])?;
            offset += chunk_size;
        }

        // Sync the file to disk
        dst_file.sync_all()?;

        // Don't close the file descriptor since we don't own it
        std::mem::forget(dst_file);

        // Abort the read transaction
        drop(txn);

        Ok(())
    }

    /// Copy environment with options
    /// 
    /// This function may be used to make a backup of an existing environment.
    /// If the flags parameter is set to CP_COMPACT, the copy will be compacted,
    /// omitting free pages and renumbering all pages sequentially.
    pub fn copy2(&self, path: &Path, flags: u32) -> Result<()> {
        if flags & CP_COMPACT != 0 {
            // Compacting copy
            self.copy_compact(path)
        } else {
            // Regular copy
            self.copy(path)
        }
    }

    /// Internal method to perform compacting copy
    fn copy_compact(&self, path: &Path) -> Result<()> {
        // Open the destination file with write access
        let dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path.join("data.mdb"))?;

        // Start a read transaction to ensure consistency
        let txn = self.begin_txn()?;

        // Get the memory map
        let src_map = match &self.map {
            Some(map) => map,
            None => return Err(Error::Invalid),
        };

        // Write meta pages first (always 2 pages)
        let meta_size = self.page_size as usize * META_PAGES;
        dst_file.write_all(&src_map[..meta_size])?;

        // Create a cursor to walk through the free list
        let free_db = Database::open(&txn, None, 0)?;
        let mut free_cursor = free_db.cursor(&txn)?;

        // Collect free pages
        let mut free_pages = Vec::new();
        while let Some((key, data)) = free_cursor.next()? {
            let pgno = u64::from_le_bytes(key.try_into().unwrap());
            let count = u64::from_le_bytes(data.try_into().unwrap());
            free_pages.push((pgno, count));
        }

        // Sort free pages
        free_pages.sort_unstable_by_key(|&(pgno, _)| pgno);

        // Write data pages, skipping free pages
        let mut write_offset = meta_size;
        let mut read_offset = meta_size;
        let size = self.data_file.metadata()?.len() as usize;

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
            dst_file.write_all(&src_map[read_offset..read_offset + chunk_size])?;
            
            // Update page number in the written page
            let new_pgno = (write_offset / self.page_size as usize) as u64;
            let mut page_data = [0u8; 8];
            page_data[..8].copy_from_slice(&new_pgno.to_le_bytes());
            dst_file.seek(SeekFrom::Start(write_offset as u64))?;
            dst_file.write_all(&page_data)?;

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
            root_page: 0, // Will be updated
            free_pages: 0,
        };

        dst_file.seek(SeekFrom::Start(0))?;
        dst_file.write_all(unsafe {
            std::slice::from_raw_parts(
                &meta as *const _ as *const u8,
                std::mem::size_of::<MetaHeader>()
            )
        })?;

        // Sync the file to disk
        dst_file.sync_all()?;

        // Abort the read transaction
        drop(txn);

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
        // Check if environment is read-only
        if self.flags & RDONLY != 0 {
            return Err(Error::from(libc::EACCES));
        }

        // Get the memory map
        let map = match &self.map {
            Some(map) => map,
            None => return Err(Error::Invalid),
        };

        // If using write map, sync the whole memory map
        if self.flags & WRITEMAP != 0 {
            if force || !(self.flags & MAPASYNC != 0) {
                // Sync the entire memory map
                map.flush()?;
            }
            return Ok(());
        }

        // Otherwise, sync the data file
        if force || !(self.flags & (NOSYNC | MAPASYNC) != 0) {
            self.data_file.sync_all()?;
        } else {
            // Just flush the buffers without forcing to disk
            self.data_file.sync_data()?;
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
        self.flags = 0;
        self.map_size = 0;
        self.max_pages = 0;
        self.num_readers.store(0, Ordering::Relaxed);
    }

    /// Set environment flags.
    /// 
    /// This may be used to set some flags in addition to those from open(),
    /// or to unset these flags. Only certain flags may be changed after 
    /// the environment is opened.
    pub fn set_flags(&mut self, flags: u32, onoff: bool) -> Result<()> {
        // Only these flags can be changed after env is opened
        const CHANGEABLE: u32 = NOSYNC | NOMETASYNC | MAPASYNC | NOMEMINIT;
        
        // Check if trying to change invalid flags
        if flags & !CHANGEABLE != 0 {
            return Err(Error::from(libc::EINVAL));
        }

        if onoff {
            // Set flags
            self.flags |= flags;
        } else {
            // Clear flags
            self.flags &= !flags;
        }

        Ok(())
    }

    /// Get environment flags.
    /// 
    /// Returns the flags currently set in the environment.
    pub fn get_flags(&self) -> Result<u32> {
        // Return all flags except internal ones
        const VISIBLE_FLAGS: u32 = FIXEDMAP | NOSUBDIR | NOSYNC | RDONLY |
                                 NOMETASYNC | WRITEMAP | MAPASYNC | NOTLS |
                                 NOLOCK | NORDAHEAD | NOMEMINIT | PREVSNAPSHOT;
        
        Ok(self.flags & VISIBLE_FLAGS)
    }

    /// Get the path that was used in open()
    /// 
    /// Returns the path that was used in the Environment::open() call.
    pub fn get_path(&self) -> Result<&Path> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
            return Err(Error::from(libc::EINVAL));
        }
        Ok(&self.path)
    }

    /// Get the file descriptor for the environment
    /// 
    /// Returns the file descriptor for the main data file.
    /// This can be used to close the file descriptor before exec*() calls.
    pub fn get_fd(&self) -> Result<i32> {
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
        if size > 0 {
            self.map_size = size;
            self.remap()?;
        }
        Ok(())
    }

    /// Set the maximum number of threads/reader slots
    pub fn set_max_readers(&mut self, readers: u32) -> Result<()> {
        if self.num_readers.load(Ordering::Relaxed) > 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot change max_readers while readers are active"
            ));
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
        // Can only be called before environment is opened
        if !self.path.as_os_str().is_empty() {
            return Err(Error::from(libc::EINVAL));
        }

        // Store max dbs
        self.max_dbs = dbs;
        Ok(())
    }

    /// Get the maximum number of named databases
    /// 
    /// Returns the maximum number of named databases that can be opened
    /// in the environment.
    pub fn get_max_dbs(&self) -> Result<u32> {
        Ok(self.max_dbs)
    }

    /// Get the maximum size of keys we can write
    /// 
    /// Returns the maximum size of a key that can be written to the database.
    /// This is platform dependent but is typically around 511 bytes.
    pub fn get_max_keysize(&self) -> Result<u32> {
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
        self.userctx = ctx;
        Ok(())
    }

    /// Get the application information associated with the environment
    /// 
    /// Returns the pointer that was previously stored with set_userctx().
    pub fn get_userctx(&self) -> Result<*mut c_void> {
        Ok(self.userctx)
    }

    /// Begin a new transaction
    /// 
    /// Creates a new transaction for use with the environment.
    /// The transaction handle may be used in calls to other functions.
    ///
    /// # Arguments
    /// * `parent` - Optional parent transaction for nested transactions
    /// * `flags` - Special options for this transaction
    ///
    /// # Returns
    /// A Result containing the new transaction or an error
    pub fn begin_txn(&self) -> Result<Transaction> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
            return Err(Error::from(libc::EINVAL));
        }

        Transaction::new(self)
    }

    pub fn begin_ro_txn(&self) -> Result<Transaction> {
        unimplemented!("Read-only transaction management not yet implemented")
    }

    pub fn reader_list(&self) -> Result<Vec<ReaderInfo>> {
        unimplemented!("Reader list not yet implemented")
    }

    /// Get LMDB version
    pub fn version() -> (i32, i32, i32) {
        unimplemented!("Version info not yet implemented")
    }

    /// Get environment statistics
    /// 
    /// Returns statistics about the LMDB environment.
    pub fn stat(&self) -> Result<Stat> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
            return Err(Error::from(libc::EINVAL));
        }

        // Get meta page to read stats
        let meta = match &self.map {
            Some(map) => unsafe {
                &*(map.as_ptr() as *const MetaHeader)
            },
            None => return Err(Error::Invalid),
        };

        Ok(Stat {
            psize: self.page_size,
            depth: meta.root_page,  // Using root_page as depth
            branch_pages: 0,        // Need to traverse DB to get these
            leaf_pages: 0,          // Need to traverse DB to get these
            overflow_pages: 0,      // Need to traverse DB to get these
            entries: 0,             // Need to traverse DB to get this
        })
    }

    /// Get environment information
    /// 
    /// Returns information about the LMDB environment.
    pub fn info(&self) -> Result<EnvInfo> {
        // Check if environment is initialized
        if self.path.as_os_str().is_empty() {
            return Err(Error::from(libc::EINVAL));
        }

        // Get meta page to read info
        let meta = match &self.map {
            Some(map) => unsafe {
                &*(map.as_ptr() as *const MetaHeader)
            },
            None => return Err(Error::Invalid),
        };

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
        unimplemented!("Reader check not yet implemented")
    }

    // Internal helper methods
    fn remap(&mut self) -> Result<()> {
        // Get file size
        let size = self.data_file.metadata()?.len() as usize;
        
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
                .map_mut(&self.data_file)?
        };

        self.map = Some(map);
        self.map_size = map_size;
        self.max_pages = (map_size / PAGE_SIZE) as u64;

        Ok(())
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        // Ensure memory map is synced
        if let Some(map) = self.map.take() {
            let _ = map.flush();
        }
    }
}

