// Page and version constants
pub const PAGE_SIZE: usize = 4096;
pub const MDB_MAGIC: u32 = 0xBEEF_DEAD;
pub const MDB_VERSION: u32 = 1;
pub const VERSION_MAJOR: u32 = 0;
pub const VERSION_MINOR: u32 = 9;
pub const VERSION_PATCH: u32 = 70;

// Environment flags
pub const FIXEDMAP: u32 = 0x01;
pub const NOSUBDIR: u32 = 0x4000;
pub const NOSYNC: u32 = 0x10000;
pub const RDONLY: u32 = 0x20000;
pub const NOMETASYNC: u32 = 0x40000;
pub const WRITEMAP: u32 = 0x80000;
pub const MAPASYNC: u32 = 0x100000;
pub const NOTLS: u32 = 0x200000;
// ... other env flags

// Database flags
pub const REVERSEKEY: u32 = 0x02;
pub const DUPSORT: u32 = 0x04;
pub const INTEGERKEY: u32 = 0x08;
pub const DUPFIXED: u32 = 0x10;
pub const INTEGERDUP: u32 = 0x20;
pub const REVERSEDUP: u32 = 0x40;
pub const CREATE: u32 = 0x40000;
// ... other db flags

// Write flags
pub const NOOVERWRITE: u32 = 0x10;
pub const NODUPDATA: u32 = 0x20;
pub const CURRENT: u32 = 0x40;
pub const RESERVE: u32 = 0x10000;
pub const APPEND: u32 = 0x20000;
pub const APPENDDUP: u32 = 0x40000;
pub const MULTIPLE: u32 = 0x80000;
// ... other write flags

// Internal constants
pub const META_PAGES: usize = 2;

// Node flags
pub const F_BIGDATA: u32 = 0x01;
pub const F_DUPDATA: u32 = 0x02;
pub const F_SUBDATA: u32 = 0x04;
pub const F_DIRTY: u32 = 0x08;

// Copy flags
pub const CP_COMPACT: u32 = 0x01; 