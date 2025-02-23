use crate::constants::PageFlags;

/// Database info structure matching MDB_db
#[derive(Debug, Clone)]
pub(crate) struct DbInfo {
    /// Page size for this database
    pub page_size: u32,
    /// Database flags
    pub flags: u32,
    /// Depth of B-tree
    pub depth: u16,
    /// Number of branch pages
    pub branch_pages: u64,
    /// Number of leaf pages
    pub leaf_pages: u64,
    /// Number of overflow pages
    pub overflow_pages: u64,
    /// Number of data entries
    pub entries: usize,
    /// Root page number
    pub root: u64,
}

/// Meta header matching MDB_meta
#[derive(Debug, Clone)]
pub(crate) struct MetaHeader {
    /// Magic number identifying LMDB file
    pub(crate) magic: u32,
    /// Version number
    pub(crate) version: u32,
    /// Format ID
    pub(crate) format_id: u32,
    /// Page size for this database
    pub(crate) page_size: u32,
    /// Address for fixed mapping
    pub(crate) address: usize,
    /// Size of mmap region
    pub(crate) mapsize: usize,
    /// First is free space, 2nd is main db
    pub(crate) dbs: Vec<u64>,
    /// Last used page in the datafile
    pub(crate) last_pgno: u64,
    /// txnid that committed this page
    pub(crate) last_txn_id: u64,
    /// Root page number
    pub(crate) root_page: u64,
    /// Number of free pages
    pub(crate) free_pages: u64,
    /// txnid that committed this page
    pub(crate) txnid: u64,
}

/// Page header matching MDB_page
#[derive(Debug, Clone)]
pub(crate) struct PageHeader {
    /// Page number
    pub(crate) pgno: u64,
    /// For in-memory list of freed pages
    pub(crate) next: u64,
    /// Page type flags
    pub(crate) flags: PageFlags,
    /// Lower bound of free space
    pub(crate) lower: u16,
    /// Upper bound of free space
    pub(crate) upper: u16,
}

/// Meta page matching MDB_metabuf
#[derive(Debug, Clone)]
pub(crate) struct MetaPage {
    /// Page header
    pub(crate) header: MetaHeader,
    /// Meta data
    pub(crate) data: Vec<u8>,
}

/// Reader status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReaderStatus {
    Active,
    Inactive,
    Closed,
}

/// Reader info
#[derive(Debug)]
pub(crate) struct ReaderInfo {
    /// Process ID
    pub pid: u32,
    /// Transaction ID being read
    pub txn_id: u64,
    /// Reader lock status
    pub status: ReaderStatus,
}

impl Clone for ReaderInfo {
    fn clone(&self) -> Self {
        Self {
            pid: self.pid,
            txn_id: self.txn_id,
            status: self.status,
        }
    }
}

/// Reader lock table matching MDB_txninfo
#[derive(Debug, Clone)]
pub(crate) struct ReaderTable {
    /// Number of slots in use
    pub num_readers: u32,
    /// Maximum number of readers
    pub max_readers: u32,
    /// Array of reader slots
    pub readers: Vec<ReaderInfo>,
}

/// Database statistics matching MDB_stat
#[derive(Debug, Clone, Default)]
pub(crate) struct Stat {
    /// Size of a database page
    pub(crate) psize: u32,
    /// Depth (height) of the B-tree
    pub(crate) depth: u32,
    /// Number of internal (non-leaf) pages
    pub(crate) branch_pages: usize,
    /// Number of leaf pages
    pub(crate) leaf_pages: usize,
    /// Number of overflow pages
    pub(crate) overflow_pages: usize,
    /// Number of data entries
    pub(crate) entries: usize,
}

impl Default for DbInfo {
    fn default() -> Self {
        Self {
            page_size: crate::constants::PAGE_SIZE as u32,
            flags: 0,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
            root: 0,
        }
    }
}

impl Default for MetaHeader {
    fn default() -> Self {
        Self {
            magic: crate::constants::MDB_MAGIC,
            version: crate::constants::VERSION_MAJOR << 24 | 
                    crate::constants::VERSION_MINOR << 16 | 
                    crate::constants::VERSION_PATCH,
            format_id: 0,
            page_size: crate::constants::PAGE_SIZE as u32,
            address: 0,
            mapsize: 0,
            dbs: Vec::new(),
            last_pgno: 0,
            last_txn_id: 0,
            root_page: 0,
            free_pages: 0,
            txnid: 0,
        }
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            pgno: 0,
            next: 0,
            flags: PageFlags::empty(),
            lower: 0,
            upper: 0,
        }
    }
}

impl Default for MetaPage {
    fn default() -> Self {
        Self {
            header: MetaHeader::default(),
            data: Vec::new(),
        }
    }
}

impl Default for ReaderInfo {
    fn default() -> Self {
        Self {
            pid: 0,
            txn_id: 0,
            status: ReaderStatus::Inactive,
        }
    }
}

impl Default for ReaderTable {
    fn default() -> Self {
        Self {
            num_readers: 0,
            max_readers: 0,
            readers: Vec::new(),
        }
    }
} 