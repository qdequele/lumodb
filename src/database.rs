use std::ffi::CString;
use std::ptr::null_mut;
use std::ptr::NonNull;
use std::ptr::NonNull::new;
use std::ptr::NonNull::new_unchecked;

use crate::cursor::Cursor;
use crate::transaction::Transaction;
use crate::error::{Error, Result};
use crate::value::Value;

/// Database handle
pub struct Database {
    /// Database identifier
    dbi: u32,
    /// Database flags
    flags: u32,
    /// Database depth
    depth: u16,
    /// Number of branch pages
    branch_pages: usize,
    /// Number of leaf pages
    leaf_pages: usize,
    /// Number of overflow pages
    overflow_pages: usize,
    /// Number of entries
    entries: usize,
    /// Root page number
    root: u64,
    /// Key size for LEAF2 pages
    pad: u32,
}

impl Database {
    /// Open a database in the environment
    pub fn open(txn: &Transaction, name: Option<&str>, flags: u32) -> Result<Self> {
        let mut dbi: u32 = 0;
        
        // Convert name to CString if provided
        let name_cstr = match name {
            Some(n) => Some(CString::new(n)?),
            None => None,
        };
        
        // Get next available database ID from transaction
        dbi = txn.allocate_dbi()?;
        
        // Create new database with allocated ID
        let db = Database {
            dbi,
            flags,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0, 
            overflow_pages: 0,
            entries: 0,
            root: 0,
            pad: 0,
        };

        // Register database in transaction
        txn.register_database(&db, name_cstr.as_ref())?;

        Ok(db)
    }

    /// Close the database
    pub fn close(&mut self) -> Result<()> {
        // Reset all database fields to their default values
        self.dbi = 0;
        self.flags = 0;
        self.depth = 0;
        self.branch_pages = 0;
        self.leaf_pages = 0;
        self.overflow_pages = 0;
        self.entries = 0;
        self.root = 0;
        self.pad = 0;
        
        Ok(())
    }

    /// Get database statistics
    pub fn stat(&self, txn: &Transaction) -> Result<Stat> {
        // Get stats from transaction's page database
        let stats = txn.get_db_stats(self.dbi)?;
        
        Ok(Stat {
            psize: stats.page_size,
            depth: stats.depth,
            branch_pages: stats.branch_pages,
            leaf_pages: stats.leaf_pages,
            overflow_pages: stats.overflow_pages,
            entries: stats.entries,
        })
    }

    /// Get database flags
    pub fn flags(&self, txn: &Transaction) -> Result<u32> {
        let mut flags: u32 = 0;
        
        let rc = unsafe {
            mdb_dbi_flags(txn as *const _ as *mut _, self.dbi, &mut flags)
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(flags)
    }

    /// Drop a database
    pub fn drop(&self, txn: &Transaction, del: bool) -> Result<()> {
        let rc = unsafe {
            mdb_drop(txn as *const _ as *mut _, self.dbi, del as i32)
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, txn: &Transaction, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut key_val = MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut _,
        };
        
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let rc = unsafe {
            mdb_get(txn as *const _ as *mut _, self.dbi, &mut key_val, &mut data_val)
        };

        match rc {
            0 => {
                let data = unsafe {
                    std::slice::from_raw_parts(
                        data_val.mv_data as *const u8,
                        data_val.mv_size
                    ).to_vec()
                };
                Ok(Some(data))
            }
            // Key not found
            -30798 => Ok(None),
            // Other error
            _ => Err(Error::from(rc)),
        }
    }

    /// Put a key/value pair
    pub fn put(&self, txn: &Transaction, key: &[u8], data: &[u8], flags: u32) -> Result<()> {
        let mut key_val = MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut _,
        };
        
        let mut data_val = MDB_val {
            mv_size: data.len(),
            mv_data: data.as_ptr() as *mut _,
        };

        let rc = unsafe {
            mdb_put(
                txn as *const _ as *mut _,
                self.dbi,
                &mut key_val,
                &mut data_val,
                flags
            )
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(())
    }

    /// Delete a key/value pair
    pub fn del(&self, txn: &Transaction, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        let mut key_val = MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut _,
        };
        
        let mut data_val = match data {
            Some(d) => MDB_val {
                mv_size: d.len(),
                mv_data: d.as_ptr() as *mut _,
            },
            None => MDB_val {
                mv_size: 0,
                mv_data: std::ptr::null_mut(),
            },
        };

        let rc = unsafe {
            mdb_del(
                txn as *const _ as *mut _,
                self.dbi,
                &mut key_val,
                data.is_some().then(|| &mut data_val as *mut _).unwrap_or(std::ptr::null_mut())
            )
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(())
    }

    /// Create a cursor for this database
    pub fn cursor(&self, txn: &Transaction) -> Result<Cursor> {
        let mut cursor = std::ptr::null_mut();
        
        let rc = unsafe {
            mdb_cursor_open(txn as *const _ as *mut _, self.dbi, &mut cursor)
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(Cursor {
            txn: txn as *const _ as *mut Transaction,
            dbi: self.dbi,
            page: None,
            pos: 0,
            stack: Vec::new(),
            flags: CursorFlags::empty(),
            db: self as *const _,
            xcursor: None,
        })
    }

    /// Set custom comparison function
    pub fn set_compare<F>(&self, txn: &Transaction, cmp: F) -> Result<()>
    where F: Fn(&[u8], &[u8]) -> std::cmp::Ordering {
        // Store comparison function in transaction
        let rc = unsafe {
            mdb_set_compare(txn as *const _ as *mut _, self.dbi, Some(cmp_callback::<F>))
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(())
    }

    /// Set custom duplicate data comparison function
    pub fn set_dupsort<F>(&self, txn: &Transaction, cmp: F) -> Result<()>
    where F: Fn(&[u8], &[u8]) -> std::cmp::Ordering {
        let rc = unsafe {
            mdb_set_dupsort(txn as *const _ as *mut _, self.dbi, Some(cmp_callback::<F>))
        };

        if rc != 0 {
            return Err(Error::from(rc));
        }

        Ok(())
    }
}

// Helper function for comparison callbacks
extern "C" fn cmp_callback<F>(a: *const MDB_val, b: *const MDB_val) -> i32 
where F: Fn(&[u8], &[u8]) -> std::cmp::Ordering {
    unsafe {
        let a_slice = std::slice::from_raw_parts(
            (*a).mv_data as *const u8,
            (*a).mv_size
        );
        let b_slice = std::slice::from_raw_parts(
            (*b).mv_data as *const u8,
            (*b).mv_size
        );
        
        match (a_slice.cmp(b_slice)) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Clean up any resources if needed
    }
}
