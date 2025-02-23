use bitflags::bitflags;

// Environment flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct EnvFlags: u32 {
        const FIXEDMAP = 0x01;
        const NOSUBDIR = 0x4000;
        const NOSYNC = 0x10000;
        const RDONLY = 0x20000;
        const NOMETASYNC = 0x40000;
        const WRITEMAP = 0x80000;
        const MAPASYNC = 0x100000;
        const NOTLS = 0x200000;
        const NOLOCK = 0x400000;
        const NORDAHEAD = 0x800000;
        const NOMEMINIT = 0x1000000;
        const PREVSNAPSHOT = 0x2000000;
    }
}

// Database flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct DbFlags: u32 {
        const REVERSEKEY = 0x02;
        const DUPSORT = 0x04;
        const INTEGERKEY = 0x08;
        const DUPFIXED = 0x10;
        const INTEGERDUP = 0x20;
        const REVERSEDUP = 0x40;
        const CREATE = 0x40000;
        const RDONLY = 0x20000;
        const NOOVERWRITE = 0x10;
    }
}

// Write operation flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct WriteFlags: u32 {
        const NOOVERWRITE = 0x10;
        const NODUPDATA = 0x20;
        const CURRENT = 0x40;
        const RESERVE = 0x10000;
        const APPEND = 0x20000;
        const APPENDDUP = 0x40000;
        const MULTIPLE = 0x80000;
    }
}

// Transaction flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct TransactionFlags: u32 {
        const RDONLY = 0x20000;
        const ERROR = 0x02;
        const NOSYNC = 0x10000;
        const NOMETASYNC = 0x40000;
        const FINISHED = 0x01;
        const HAS_CHILD = 0x04;
        const SPILLS = 0x08;
        const CORRUPTED = 0x10;
        const WRITEMAP = 0x20;
    }
}

// Copy operation flags
bitflags! {
    #[derive(Debug, Clone, PartialEq)]
    pub struct CopyFlags: u32 {
        const COMPACT = 0x01;
    }
}

// Internal node flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct NodeFlags: u32 {
        const BIGDATA = 0x01;
        const SUBDATA = 0x02;
        const DUPDATA = 0x04;
    }
}

// Page flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct PageFlags: u32 {
        const BRANCH = 0x01;
        const LEAF = 0x02;
        const OVERFLOW = 0x04;
        const META = 0x08;
        const DIRTY = 0x10;
        const LEAF2 = 0x20;
        const SUBP = 0x40;
        const LOOSE = 0x4000;
        const KEEP = 0x8000;
    }
}

// Cursor flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct CursorFlags: u32 {
        const FIRST = 0x0;
        const FIRST_DUP = 0x1;
        const LAST = 0x2;
        const LAST_DUP = 0x3;
        const NEXT = 0x4;
        const NEXT_DUP = 0x5;
        const NEXT_NODUP = 0x6;
        const PREV = 0x7;
        const PREV_DUP = 0x8;
        const PREV_NODUP = 0x9;
        const SET = 0xA;
        const SET_KEY = 0xB;
        const SET_RANGE = 0xC;
        const PREV_MULTIPLE = 0xD;
        const SET_MULTIPLE = 0xE;
        const SET_RANGE_MULTIPLE = 0xF;
        const INITIALIZED = 0x100;
    }
}

// Page and version constants
pub const PAGE_SIZE: usize = 4096;
/// Magic number for LMDB files
pub const MDB_MAGIC: u32 = 0xBEEF_DEAD;
/// Version numbers major
pub const VERSION_MAJOR: u32 = 0;
/// Version numbers minor
pub const VERSION_MINOR: u32 = 9;
/// Version numbers patch
pub const VERSION_PATCH: u32 = 70;
/// Core database identifiers reserved by LMDB
pub const CORE_DBS: u32 = 2;
/// Internal constants
pub const META_PAGES: usize = 2;

// Default values
pub const DEFAULT_PAGE_SIZE: usize = 4096;
pub const DEFAULT_MAX_READERS: u32 = 126;
pub const DEFAULT_MAX_DBS: u32 = 32;
pub const DEFAULT_MAP_SIZE: usize = 10485760;
pub const DEFAULT_MAX_KEY_SIZE: usize = 511;
pub const DEFAULT_MAX_CURSORS: u32 = 1024;
pub const DEFAULT_MAX_DIRTY: u32 = 1024;
pub const DEFAULT_READER_CHECK_SECS: u64 = 60;
