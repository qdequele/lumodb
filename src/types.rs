use bitflags::bitflags;
use std::os::raw::c_void;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DbFlags {
    pub reverse_key: bool,
    pub integer_key: bool,
    pub duplicate_keys: bool,
    pub integer_dup: bool,
    pub reverse_dup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CursorOp {
    First,
    FirstDup,
    GetBoth,
    GetBothRange,
    GetCurrent,
    GetMultiple,
    Last,
    LastDup,
    Next,
    NextDup,
    NextMultiple,
    NextNoDup,
    Prev,
    PrevDup,
    PrevNoDup,
    Set,
    SetKey,
    SetRange,
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

#[derive(Debug, Clone)]
pub struct EnvInfo {
    pub mapaddr: *mut c_void,
    pub mapsize: usize,
    pub last_pgno: usize,
    pub last_txnid: usize,
    pub max_readers: u32,
    pub num_readers: u32,
} 