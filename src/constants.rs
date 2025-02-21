use bitflags::bitflags;

// Environment flags
bitflags! {
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
    pub struct DbFlags: u32 {
        const REVERSEKEY = 0x02;
        const DUPSORT = 0x04;
        const INTEGERKEY = 0x08;
        const DUPFIXED = 0x10;
        const INTEGERDUP = 0x20;
        const REVERSEDUP = 0x40;
        const CREATE = 0x40000;
    }
}

// Write operation flags
bitflags! {
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
    pub struct TransactionFlags: u32 {
        // Reuse environment flags
        const RDONLY = EnvFlags::RDONLY.bits();
        const NOSYNC = EnvFlags::NOSYNC.bits();
        const NOMETASYNC = EnvFlags::NOMETASYNC.bits();
        
        // Transaction-specific flags
        const FINISHED = 0x01;
        const ERROR = 0x02;
        const DIRTY = 0x04;
        const SPILLS = 0x08;
        const HAS_CHILD = 0x10;
    }
}


// Copy operation flags
bitflags! {
    pub struct CopyFlags: u32 {
        const COMPACT = 0x01;
    }
}

// Internal node flags
bitflags! {
    pub struct NodeFlags: u32 {
        const BIGDATA = 0x01;
        const DUPDATA = 0x02;
        const SUBDATA = 0x04;
        const DIRTY = 0x08;
    }
}

// Page flags
bitflags! {
    pub struct PageFlags: u16 {
        const LEAF = 0x01;
        const LEAF2 = 0x02;
        const OVERFLOW = 0x04;
        const META = 0x08;
        const DIRTY = 0x10;
        const BRANCH = 0x20;
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
