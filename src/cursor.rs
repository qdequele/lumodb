use std::ptr::NonNull;
use bitflags::bitflags;
use libc;
use std::cell::RefCell;

use crate::database::Database;
use crate::transaction::Transaction;
use crate::error::{Error, Result};
use crate::value::Value;

// Constants from LMDB
const RDONLY: u32 = 0x20000;
const DUPSORT: u32 = 0x04;
const CURRENT: u32 = 0x04;
const APPEND: u32 = 0x20;
const NOOVERWRITE: u32 = 0x10;

// Page flags
const P_LEAF: u16 = 0x01;
const P_LEAF2: u16 = 0x02;

// Node flags
const F_DUPDATA: u16 = 0x01;
const F_BIGDATA: u16 = 0x02;

#[repr(C)]
struct Page {
    mp_flags: u16,
    mp_lower: u16,
    mp_ptrs: [NonNull<Node>; 0],
    mp_data: [u8; 0],
}

#[repr(C)]
struct Node {
    mn_flags: u16,
    mn_ksize: u32,
    mn_size: u32,
    mn_key: [u8; 0],
    mn_data: [u8; 0],
}

// Remove external C functions and just keep the operation constants 
// that we'll use in our pure Rust implementation
const GET_BOTH: u32 = 0x8;
const GET_BOTH_RANGE: u32 = 0x10;

/// Internal cursor state
struct CursorState {
    /// Current page number
    page: Option<NonNull<Page>>,
    /// Current position on page
    pos: usize,
    /// Stack of parent pages for current position
    stack: Vec<(NonNull<Page>, usize)>,
    /// Cursor state flags
    flags: CursorFlags,
    /// For sorted-duplicate databases
    xcursor: Option<Box<XCursor>>,
}

/// Database cursor
pub struct Cursor<'txn> {
    /// The transaction this cursor belongs to
    txn: &'txn Transaction,
    /// The database this cursor operates on
    dbi: u32,
    /// Reference to database
    db: *const Database,
    /// Mutable cursor state
    state: RefCell<CursorState>,
}

/// Cursor state flags
bitflags! {
    pub struct CursorFlags: u32 {
        const INITIALIZED = 0x01;
        const EOF = 0x02;
        const SUBDB = 0x04;
        const DELETED = 0x08;
        const MULTIPLE = 0x10;
    }
}

/// Extended cursor for sorted duplicates
pub struct XCursor {
    /// Sub-cursor for duplicate data
    cursor: Cursor,
    /// Last known data position
    last_data: Option<NonNull<Page>>,
}

struct PageCursor {
    page: NonNull<Page>,
    pos: usize,
    stack: Vec<(NonNull<Page>, usize)>,
}

impl PageCursor {
    /// Create new page cursor starting at given page
    fn new(page: NonNull<Page>) -> Self {
        PageCursor {
            page,
            pos: 0,
            stack: Vec::new(),
        }
    }

    /// Move to leftmost leaf page
    fn seek_first(&mut self, get_page: impl Fn(u32) -> Result<NonNull<Page>>) -> Result<()> {
        while !self.is_leaf() {
            // Get leftmost child
            let next = self.get_child(0, &get_page)?;
            self.stack.push((self.page, 0));
            self.page = next;
        }
        Ok(())
    }

    /// Move to rightmost leaf page
    fn seek_last(&mut self, get_page: impl Fn(u32) -> Result<NonNull<Page>>) -> Result<()> {
        while !self.is_leaf() {
            let num_ptrs = unsafe { (*self.page.as_ptr()).mp_lower as usize };
            let next = self.get_child(num_ptrs - 1, &get_page)?;
            self.stack.push((self.page, num_ptrs - 1));
            self.page = next;
        }
        // Set position to last entry
        self.pos = unsafe { (*self.page.as_ptr()).mp_lower as usize } - 1;
        Ok(())
    }

    /// Check if current page is a leaf
    fn is_leaf(&self) -> bool {
        unsafe { (*self.page.as_ptr()).mp_flags & P_LEAF != 0 }
    }

    /// Get child page at given index
    fn get_child(&self, index: usize, get_page: impl Fn(u32) -> Result<NonNull<Page>>) -> Result<NonNull<Page>> {
        // Validate page pointer
        let page_ref = unsafe { &*self.page.as_ptr() };
        
        // Check index bounds
        if index >= page_ref.mp_lower as usize {
            return Err(Error::Invalid);
        }

        // Validate node pointer
        let node_ptr = unsafe { page_ref.mp_ptrs[index].as_ptr() };
        if node_ptr.is_null() {
            return Err(Error::Invalid);
        }

        // Get node reference safely
        let node = unsafe { &*node_ptr };

        // Validate key size for page number (must be at least 4 bytes)
        if node.mn_ksize < 4 {
            return Err(Error::BadValSize);
        }

        // Create slice safely
        let pgno = unsafe {
            let key_slice = std::slice::from_raw_parts(
                node.mn_key.as_ptr() as *const u8,
                node.mn_ksize as usize
            );
            // Ensure we have enough bytes for a u32
            key_slice.get(..4)
                .ok_or(Error::BadValSize)?
                .try_into()
                .map_err(|_| Error::BadValSize)?
        };

        // Convert to page number and get the page
        let child_pgno = u32::from_le_bytes(pgno);
        get_page(child_pgno)
    }
}

impl<'txn> Cursor<'txn> {
    /// Create a new cursor
    pub fn new(txn: &'txn Transaction, db: &Database) -> Result<Self> {
        let state = CursorState {
            page: None,
            pos: 0,
            stack: Vec::new(),
            flags: CursorFlags::empty(),
            xcursor: None,
        };

        let mut cursor = Cursor {
            txn,
            dbi: db.dbi,
            db: db as *const _,
            state: RefCell::new(state),
        };

        // Initialize xcursor if needed
        if db.flags & DUPSORT != 0 {
            cursor.state.borrow_mut().xcursor = Some(Box::new(XCursor {
                cursor: Cursor::new(cursor.txn, db)?,
                last_data: None,
            }));
        }

        Ok(cursor)
    }

    /// Position at first key/data item
    pub fn first(&self) -> Result<Option<(&[u8], &[u8])>> {
        let mut state = self.state.borrow_mut();
        
        // Reset cursor state
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.state.borrow_mut().flags &= !(CursorFlags::INITIALIZED | CursorFlags::EOF);
        }

        // Search for first page
        self.search_first(&mut state)?;

        // Get current entry if any
        self.get_current(&state)
    }

    /// Position at last key/data item
    pub fn last(&self) -> Result<Option<(&[u8], &[u8])>> {
        let mut state = self.state.borrow_mut();
        
        // Reset cursor state
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.state.borrow_mut().flags &= !(CursorFlags::INITIALIZED | CursorFlags::EOF);
        }

        // Search for last page
        self.search_last(&mut state)?;

        // Get current entry if any
        self.get_current(&state)
    }

    /// Position at specified key
    pub fn set(&self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        // Validate input
        if key.is_empty() {
            return Err(Error::BadValSize);
        }

        // Validate cursor and transaction state
        self.validate_cursor()?;

        let mut state = self.state.borrow_mut();
        
        // Reset xcursor if it exists
        self.reset_xcursor(&mut state);

        // Initialize search state atomically
        self.init_search_state(&mut state)?;

        // Search for key in B-tree
        self.search_key(&state, key)?;

        // Get current entry if found
        self.get_current(&state)
    }

    /// Reset xcursor state
    fn reset_xcursor(&self, state: &mut CursorState) {
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.state.borrow_mut().flags &= !(CursorFlags::INITIALIZED | CursorFlags::EOF);
        }
    }

    /// Search for a key in the B-tree
    fn search_key(&self, state: &CursorState, key: &[u8]) -> Result<()> {
        // Reset cursor state
        self.init_search_state(state)?;
        
        // Get and validate root page
        let root_page = self.get_root_page()?;
        
        // Perform the actual search
        self.search_from_page(state, root_page, key)
    }

    /// Initialize cursor state for a new search
    fn init_search_state(&self, state: &mut CursorState) -> Result<()> {
        // Validate transaction state first
        if !self.txn.is_valid() {
            return Err(Error::BadTxn);
        }

        // Take a write lock on state to ensure atomic updates
        state.page = None;
        state.pos = 0;
        state.stack.clear();
        state.flags &= !CursorFlags::INITIALIZED;

        Ok(())
    }

    /// Get the root page for searching
    fn get_root_page(&self) -> Result<NonNull<Page>> {
        let db = unsafe { &*self.db };
        if db.root_page == 0 {
            return Err(Error::NotFound);
        }
        self.get_page(db.root_page)
    }

    /// Search for key starting from a specific page
    fn search_from_page(&self, state: &CursorState, mut page: NonNull<Page>, key: &[u8]) -> Result<()> {
        loop {
            if self.is_leaf_page(page) {
                return self.search_leaf_page(state, page, key);
            }
            
            let (next_page, pos) = self.descend_to_child(page, key)?;
            state.stack.push((page, pos));
            page = next_page;
        }
    }

    /// Search for key in a leaf page
    fn search_leaf_page(&self, state: &CursorState, page: NonNull<Page>, key: &[u8]) -> Result<()> {
        let pos = self.binary_search_page(page, key)?;
        state.page = Some(page);
        state.pos = pos;
        state.flags |= CursorFlags::INITIALIZED;
        Ok(())
    }

    /// Find child page to descend to during search
    fn descend_to_child(&self, page: NonNull<Page>, key: &[u8]) -> Result<(NonNull<Page>, usize)> {
        let pos = self.binary_search_page(page, key)?;
        let next = self.get_child(page, |pgno| self.get_page(pgno))?;
        Ok((next, pos))
    }

    /// Perform binary search within a page
    fn binary_search_page(&self, page: NonNull<Page>, key: &[u8]) -> Result<usize> {
        let page_ref = unsafe { &*page.as_ptr() };
        let mut left = 0;
        let mut right = page_ref.mp_lower as usize;

        while left < right {
            let mid = (left + right) / 2;
            let node = self.get_node(page, mid)?;
            let node_key = self.get_node_key(node)?;

            match node_key.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        Ok(left)
    }

    /// Position at key/data pair
    pub fn get_both(&self, key: &[u8], data: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if key.is_empty() || data.is_empty() {
            return Err(Error::BadValSize);
        }

        let mut state = self.state.borrow_mut();
        
        // Reset xcursor if it exists
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.state.borrow_mut().flags &= !(CursorFlags::INITIALIZED | CursorFlags::EOF);
        }

        // First search for the key
        self.search_key(&state, key)?;

        // Get current entry
        match self.get_current(&state)? {
            Some((k, d)) if k == key && d == data => Ok(Some((k, d))),
            _ => Ok(None)
        }
    }

    /// Position at key, nearest data
    pub fn get_both_range(&self, key: &[u8], data: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if key.is_empty() || data.is_empty() {
            return Err(Error::BadValSize);
        }

        let mut state = self.state.borrow_mut();
        
        // Reset xcursor if it exists
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.state.borrow_mut().flags &= !(CursorFlags::INITIALIZED | CursorFlags::EOF);
        }

        // First search for the key
        self.search_key(&state, key)?;

        // Get current entry
        match self.get_current(&state)? {
            Some((k, d)) if k == key && d >= data => Ok(Some((k, d))),
            _ => Ok(None)
        }
    }

    /// Helper methods for page validation and access
    
    /// Validate and get page reference
    fn validate_page(&self, page: Option<NonNull<Page>>) -> Result<NonNull<Page>> {
        match page {
            Some(p) => Ok(p),
            None => Err(Error::Invalid),
        }
    }

    /// Validate page position
    fn validate_position(&self, page: NonNull<Page>, pos: usize) -> Result<()> {
        let page_ref = unsafe { &*page.as_ptr() };
        if pos >= page_ref.mp_lower as usize {
            return Err(Error::Invalid);
        }
        Ok(())
    }

    /// Safely get node at position
    fn get_node(&self, page: NonNull<Page>, pos: usize) -> Result<&Node> {
        // Validate position first
        self.validate_position(page, pos)?;
        
        let page_ref = unsafe { &*page.as_ptr() };
        let node_ptr = unsafe { page_ref.mp_ptrs[pos].as_ptr() };
        
        if node_ptr.is_null() {
            return Err(Error::Invalid);
        }
        
        Ok(unsafe { &*node_ptr })
    }

    /// Check if page is a leaf page
    fn is_leaf_page(&self, page: NonNull<Page>) -> bool {
        unsafe { (*page.as_ptr()).mp_flags & P_LEAF != 0 }
    }

    /// Get page data slice safely
    fn get_page_data(&self, page: NonNull<Page>) -> Result<&[u8]> {
        let page_ref = unsafe { &*page.as_ptr() };
        Ok(unsafe {
            std::slice::from_raw_parts(
                page_ref.mp_data.as_ptr(),
                page_ref.mp_lower as usize
            )
        })
    }

    /// Get node key data safely
    fn get_node_key(&self, node: &Node) -> Result<&[u8]> {
        if node.mn_ksize == 0 {
            return Err(Error::BadValSize);
        }
        
        Ok(unsafe {
            std::slice::from_raw_parts(
                node.mn_key.as_ptr(),
                node.mn_ksize as usize
            )
        })
    }

    /// Get node value data safely
    fn get_node_data(&self, node: &Node) -> Result<&[u8]> {
        Ok(unsafe {
            std::slice::from_raw_parts(
                node.mn_data.as_ptr(),
                node.mn_size as usize
            )
        })
    }

    /// Get current key/data pair
    pub fn get_current(&self, state: &CursorState) -> Result<Option<(&[u8], &[u8])>> {
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Ok(None);
        }

        // Get and validate current page
        let page = self.validate_page(state.page)?;

        // Handle leaf2 pages (fixed-size keys)
        if unsafe { (*page.as_ptr()).mp_flags & P_LEAF2 } != 0 {
            let key_size = unsafe { (*(*self.db).md_pad) };
            let key_data = self.get_page_data(page)?;
            return Ok(Some((key_data, &[])));
        }

        // Get node and validate
        let node = self.get_node(page, state.pos)?;

        // Get data
        let data = if node.mn_flags & F_DUPDATA != 0 {
            if let Some(ref xcursor) = state.xcursor {
                match xcursor.cursor.get_current(&xcursor.cursor.state.borrow())? {
                    Some((_, data)) => data,
                    None => return Ok(None),
                }
            } else {
                return Err(Error::Invalid);
            }
        } else {
            self.get_node_data(node)?
        };

        // Get key
        let key = self.get_node_key(node)?;

        Ok(Some((key, data)))
    }

    /// Position at next data item
    pub fn next(&self) -> Result<Option<(&[u8], &[u8])>> {
        let mut state = self.state.borrow_mut();
        
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return self.first();
        }

        // If we're at EOF, nothing more to do
        if state.flags.contains(CursorFlags::EOF) {
            return Ok(None);
        }

        // Move to next position
        self.move_next(&mut state)?;

        // Get current entry if any
        self.get_current(&state)
    }

    /// Position at previous data item
    pub fn prev(&self) -> Result<Option<(&[u8], &[u8])>> {
        let mut state = self.state.borrow_mut();
        
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return self.last();
        }

        // Move to previous position
        self.move_prev(&mut state)?;

        // Get current entry if any
        self.get_current(&state)
    }

    /// Store by cursor
    pub fn put(&self, key: &[u8], data: &[u8], flags: u32) -> Result<()> {
        // Check if transaction is writable
        let txn = unsafe { &mut *self.txn };
        if txn.flags & RDONLY != 0 {
            return Err(Error::from(libc::EACCES));
        }

        // Handle different put flags
        if flags & CURRENT != 0 {
            // Overwrite current entry
            self.overwrite_current(key, data)
        } else if flags & APPEND != 0 {
            // Append at end of database
            self.append(key, data)
        } else {
            // Normal put operation
            self.insert(key, data, flags)
        }
    }

    /// Delete current key/data pair
    pub fn del(&self, flags: u32) -> Result<()> {
        let mut state = self.state.borrow_mut();
        
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Err(Error::from(libc::EINVAL));
        }

        // Check if transaction is writable
        let txn = unsafe { &mut *self.txn };
        if txn.flags & RDONLY != 0 {
            return Err(Error::from(libc::EACCES));
        }

        // Delete current entry
        self.delete_current(&state, flags)?;

        state.flags.insert(CursorFlags::DELETED);
        Ok(())
    }

    /// Return count of duplicates for current key
    pub fn count(&self) -> Result<usize> {
        let mut state = self.state.borrow_mut();
        
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Err(Error::from(libc::EINVAL));
        }

        // If database doesn't support duplicates, count is always 1
        let db = unsafe { &*self.db };
        if db.flags & DUPSORT == 0 {
            return Ok(1);
        }

        // For duplicate databases, traverse all duplicates
        let mut count = 1;
        let current_key = match self.get_current(&state)? {
            Some((key, _)) => key,
            None => return Ok(0),
        };

        // Save current position
        let saved_page = state.page;
        let saved_pos = state.pos;
        let saved_stack = state.stack.clone();

        // Count duplicates by moving forward until key changes
        while let Some((key, _)) = self.next()? {
            if key != current_key {
                break;
            }
            count += 1;
        }

        // Restore position
        state.page = saved_page;
        state.pos = saved_pos;
        state.stack = saved_stack;

        Ok(count)
    }

    /// Renew a cursor
    pub fn renew(&self, txn: &Transaction) -> Result<()> {
        let mut state = self.state.borrow_mut();
        
        // Reset cursor state
        state.page = None;
        state.pos = 0;
        state.stack.clear();
        state.flags = CursorFlags::empty();
        
        // Reset xcursor if it exists
        if let Some(ref mut xcursor) = state.xcursor {
            xcursor.cursor.renew(txn)?;
        }

        Ok(())
    }

    // Internal helper methods
    fn search_first(&self, state: &mut CursorState) -> Result<()> {
        // Reset cursor state
        state.page = None;
        state.pos = 0;
        state.stack.clear();
        
        // Get root page
        let root = unsafe { (*self.db).root_page };
        if root == 0 {
            return Ok(());
        }

        // Create page cursor and seek to first leaf
        let mut pcursor = PageCursor::new(self.get_page(root)?);
        pcursor.seek_first(|pgno| self.get_page(pgno))?;

        // Update cursor state
        state.page = Some(pcursor.page);
        state.pos = pcursor.pos;
        state.stack = pcursor.stack;
        state.flags |= CursorFlags::INITIALIZED;
        Ok(())
    }

    fn search_last(&self, state: &mut CursorState) -> Result<()> {
        // Reset cursor state
        state.page = None;
        state.pos = 0;
        state.stack.clear();
        
        // Get root page
        let root = unsafe { (*self.db).root_page };
        if root == 0 {
            return Ok(());
        }

        // Create page cursor and seek to last leaf
        let mut pcursor = PageCursor::new(self.get_page(root)?);
        pcursor.seek_last(|pgno| self.get_page(pgno))?;

        // Update cursor state
        state.page = Some(pcursor.page);
        state.pos = pcursor.pos;
        state.stack = pcursor.stack;
        state.flags |= CursorFlags::INITIALIZED;
        Ok(())
    }

    fn get_page(&self, pgno: u32) -> Result<NonNull<Page>> {
        let txn = unsafe { &*self.txn };
        
        // Get page from transaction's memory map
        let offset = (pgno as u64) * (txn.env.page_size as u64);
        let ptr = unsafe {
            txn.env.map.as_ref()
                .ok_or(Error::Invalid)?
                .as_ptr()
                .add(offset as usize) as *mut Page
        };

        // Validate page pointer
        NonNull::new(ptr).ok_or(Error::Invalid)
    }

    /// Overwrite the data of the current key/data pair
    fn overwrite_current(&self, key: &[u8], data: &[u8]) -> Result<()> {
        let mut state = self.state.borrow_mut();
        
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Err(Error::from(libc::EINVAL));
        }

        let page = state.page.ok_or(Error::Invalid)?;
        let node = unsafe { &mut *(*page.as_ptr()).mp_ptrs[state.pos].as_ptr() };

        // Check if we need to handle duplicate data
        if node.mn_flags & F_DUPDATA != 0 {
            if let Some(ref mut xcursor) = state.xcursor {
                return xcursor.cursor.overwrite_current(&xcursor.cursor.state.borrow(), key, data);
            }
            return Err(Error::Invalid);
        }

        // Check if new data fits in current space
        if data.len() <= node.mn_size as usize {
            // Copy new data directly
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    node.mn_data.as_mut_ptr(),
                    data.len()
                );
                node.mn_size = data.len() as u32;
            }
            return Ok(());
        }

        // Need to allocate new space
        let txn = unsafe { &mut *self.txn };
        let new_page = txn.alloc_page()?;
        
        // Copy data to new page
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                (*new_page.as_ptr()).mp_data.as_mut_ptr(),
                data.len()
            );
            node.mn_data = (*new_page.as_ptr()).mp_data;
            node.mn_size = data.len() as u32;
            node.mn_flags |= F_BIGDATA;
        }

        Ok(())
    }

    /// Append a key/data pair at the end of the database
    fn append(&self, key: &[u8], data: &[u8]) -> Result<()> {
        // Position cursor at last entry
        self.last()?;

        // Check if new key maintains ordering
        let mut state = self.state.borrow_mut();
        if let Some((last_key, _)) = self.get_current(&state)? {
            if key <= last_key {
                return Err(Error::KeyExist);
            }
        }

        // Insert new entry
        self.insert(key, data, 0)
    }

    /// Insert a new key/data pair
    fn insert(&self, key: &[u8], data: &[u8], flags: u32) -> Result<()> {
        let txn = unsafe { &mut *self.txn };

        // Find insertion point
        self.search_key(&self.state.borrow(), key)?;

        // Check for existing key if NOOVERWRITE
        if flags & NOOVERWRITE != 0 {
            if let Some((existing_key, _)) = self.get_current(&self.state.borrow())? {
                if key == existing_key {
                    return Err(Error::KeyExist);
                }
            }
        }

        // Allocate new node
        let node_size = std::mem::size_of::<Node>() + key.len() + data.len();
        let node_page = txn.alloc_page()?;
        let node = unsafe { &mut *(node_page.as_ptr() as *mut Node) };

        // Initialize node
        node.mn_ksize = key.len() as u32;
        node.mn_size = data.len() as u32;
        
        // Copy key and data
        unsafe {
            std::ptr::copy_nonoverlapping(
                key.as_ptr(),
                node.mn_key.as_mut_ptr(),
                key.len()
            );
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                node.mn_data.as_mut_ptr(),
                data.len()
            );
        }

        // Insert into current page
        let page = self.state.borrow().page.ok_or(Error::Invalid)?;
        unsafe {
            // Make space for new entry
            let ptr_slice = std::slice::from_raw_parts_mut(
                (*page.as_ptr()).mp_ptrs.as_mut_ptr().add(self.state.borrow().pos),
                (*page.as_ptr()).mp_lower as usize - self.state.borrow().pos
            );
            ptr_slice.copy_within(0..ptr_slice.len()-1, 1);

            // Insert new node
            (*page.as_ptr()).mp_ptrs[self.state.borrow().pos] = NonNull::new_unchecked(node as *mut _);
            (*page.as_ptr()).mp_lower += 1;
        }

        Ok(())
    }

    /// Delete the current key/data pair
    fn delete_current(&self, state: &CursorState, flags: u32) -> Result<()> {
        let page = state.page.ok_or(Error::Invalid)?;

        // Handle duplicate data if needed
        let node = unsafe { &mut *(*page.as_ptr()).mp_ptrs[state.pos].as_ptr() };
        if node.mn_flags & F_DUPDATA != 0 {
            if let Some(ref mut xcursor) = state.xcursor {
                return xcursor.cursor.delete_current(&xcursor.cursor.state.borrow(), flags);
            }
            return Err(Error::Invalid);
        }

        // Free any overflow pages
        if node.mn_flags & F_BIGDATA != 0 {
            let txn = unsafe { &mut *self.txn };
            txn.free_page(node.mn_data.as_ptr() as u64)?;
        }

        unsafe {
            // Remove entry from page
            let ptr_slice = std::slice::from_raw_parts_mut(
                (*page.as_ptr()).mp_ptrs.as_mut_ptr().add(state.pos),
                (*page.as_ptr()).mp_lower as usize - state.pos - 1
            );
            ptr_slice.copy_within(1.., 0);
            (*page.as_ptr()).mp_lower -= 1;

            // Free node
            let txn = &mut *self.txn;
            txn.free_page(node as *mut _ as u64)?;
        }

        Ok(())
    }

    /// Duplicate the cursor
    /// Returns a new cursor pointing to the same position as this one
    pub fn duplicate(&self) -> Result<Cursor<'txn>> {
        let state = self.state.borrow();
        
        // Create new cursor state
        let mut new_state = CursorState {
            page: state.page,
            pos: state.pos,
            stack: state.stack.clone(),
            flags: state.flags,
            xcursor: None,
        };

        // Duplicate xcursor if it exists
        if let Some(ref xcursor) = state.xcursor {
            new_state.xcursor = Some(Box::new(XCursor {
                cursor: xcursor.cursor.duplicate()?,
                last_data: xcursor.last_data,
            }));
        }

        Ok(Cursor {
            txn: self.txn,
            dbi: self.dbi,
            db: self.db,
            state: RefCell::new(new_state),
        })
    }

    /// Create a new cursor positioned exactly as this one
    pub fn dup_exact(&self) -> Result<Cursor<'txn>> {
        let mut new_cursor = self.duplicate()?;
        
        // Ensure the new cursor is positioned exactly like the original
        if self.state.borrow().flags.contains(CursorFlags::INITIALIZED) {
            let state = self.state.borrow();
            if let Some((key, data)) = self.get_current(&state)? {
                // Position new cursor at same key/data pair
                new_cursor.get_both(key, data)?;
            }
        }
        
        Ok(new_cursor)
    }

    /// Create a new cursor for duplicate data
    pub fn dup_data(&self) -> Result<Cursor<'txn>> {
        let state = self.state.borrow();
        
        // Check if database supports duplicates
        let db = unsafe { &*self.db };
        if db.flags & DUPSORT == 0 {
            return Err(Error::Invalid);
        }

        // Create new cursor for duplicate data
        let mut new_cursor = Cursor::new(self.txn, unsafe { &*self.db })?;
        
        // If current cursor is positioned, position new cursor at same key
        if state.flags.contains(CursorFlags::INITIALIZED) {
            if let Some((key, _)) = self.get_current(&state)? {
                new_cursor.set(key)?;
            }
        }

        Ok(new_cursor)
    }

    /// Get number of duplicate data items
    pub fn count_duplicates(&self) -> Result<usize> {
        let state = self.state.borrow();
        
        // Check if cursor is initialized
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Ok(0);
        }

        // Check if database supports duplicates
        let db = unsafe { &*self.db };
        if db.flags & DUPSORT == 0 {
            return Ok(1);
        }

        // Count duplicates using xcursor
        if let Some(ref xcursor) = state.xcursor {
            let mut count = 0;
            let mut dup_cursor = xcursor.cursor.duplicate()?;
            
            // Position at first duplicate
            if dup_cursor.first()?.is_some() {
                count += 1;
                // Count remaining duplicates
                while dup_cursor.next()?.is_some() {
                    count += 1;
                }
            }
            
            Ok(count)
        } else {
            Ok(1)
        }
    }

    /// Bulk put operation for multiple key/data pairs
    pub fn put_multiple(&self, pairs: &[(Vec<u8>, Vec<u8>)], flags: u32) -> Result<()> {
        // Check if transaction is writable
        let txn = unsafe { &mut *self.txn };
        if txn.flags & RDONLY != 0 {
            return Err(Error::from(libc::EACCES));
        }

        // Sort pairs by key for optimal insertion
        let mut sorted_pairs = pairs.to_vec();
        sorted_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        // Set cursor to appropriate starting position
        if flags & APPEND != 0 {
            self.last()?;
        }

        // Track current page to minimize page loads
        let mut current_page = None;
        
        for (key, data) in sorted_pairs {
            if current_page != self.state.borrow().page {
                // Page changed, need to search for new position
                self.search_key(&self.state.borrow(), &key)?;
                current_page = self.state.borrow().page;
            }
            
            // Insert the pair
            self.insert(&key, &data, flags)?;
        }

        Ok(())
    }

    /// Bulk delete operation for multiple keys
    pub fn del_multiple(&self, keys: &[Vec<u8>], flags: u32) -> Result<()> {
        // Check if transaction is writable
        let txn = unsafe { &mut *self.txn };
        if txn.flags & RDONLY != 0 {
            return Err(Error::from(libc::EACCES));
        }

        // Sort keys for optimal deletion
        let mut sorted_keys = keys.to_vec();
        sorted_keys.sort();

        for key in sorted_keys {
            // Position cursor at key
            if let Some(_) = self.set(&key)? {
                // Delete if found
                self.del(flags)?;
            }
        }

        Ok(())
    }

    /// Bulk fetch operation for multiple keys
    pub fn get_multiple(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            match self.set(key)? {
                Some((_, data)) => results.push(Some(data.to_vec())),
                None => results.push(None),
            }
        }

        Ok(results)
    }

    /// Iterator over a range of keys
    pub fn iter_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<CursorIterator<'txn>> {
        // Position cursor at start key
        self.set(start_key)?;
        
        Ok(CursorIterator {
            cursor: self,
            end_key: end_key.to_vec(),
            finished: false,
        })
    }

    /// Validate cursor state
    fn validate_cursor(&self) -> Result<()> {
        let state = self.state.borrow();
        
        // Check if cursor is initialized
        if !state.flags.contains(CursorFlags::INITIALIZED) {
            return Err(Error::Invalid);
        }

        // Validate transaction state
        if !self.txn.is_valid() {
            return Err(Error::BadTxn);
        }

        // Validate database reference
        if self.db.is_null() {
            return Err(Error::Invalid);
        }

        Ok(())
    }
}

/// Iterator for cursor ranges
pub struct CursorIterator<'txn> {
    cursor: &'txn Cursor<'txn>,
    end_key: Vec<u8>,
    finished: bool,
}

impl<'txn> Iterator for CursorIterator<'txn> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.cursor.get_current(&self.cursor.state.borrow()) {
            Ok(Some((key, data))) => {
                if key > &self.end_key {
                    self.finished = true;
                    return None;
                }

                // Move cursor to next position
                if let Err(e) = self.cursor.next() {
                    self.finished = true;
                    return Some(Err(e));
                }

                Some(Ok((key.to_vec(), data.to_vec())))
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        // Get state with a catch for panic during borrow
        let state_result = std::panic::catch_unwind(|| self.state.try_borrow_mut());
        
        match state_result {
            Ok(Ok(mut state)) => {
                // Clean up regardless of initialization status
                // Clear all pages and references
                state.page = None;
                state.pos = 0;
                state.stack.clear();
                
                // Handle xcursor cleanup first to prevent resource leaks
                if let Some(xcursor) = state.xcursor.take() {
                    // Explicitly drop xcursor to ensure its resources are freed
                    drop(xcursor);
                }
                
                // Clear flags last after cleanup is done
                state.flags = CursorFlags::empty();
                
                // Ensure transaction knows cursor is gone
                if let Some(txn) = unsafe { self.txn.as_ref() } {
                    // Optional: Notify transaction that cursor is dropped
                    // This would require adding a method to Transaction
                    // txn.remove_cursor(self.dbi);
                }
            },
            // Handle panic or borrow failure gracefully
            _ => {
                // Log error or set panic state if needed
                // We can't do much if we can't access the state
                #[cfg(debug_assertions)]
                eprintln!("Warning: Cursor dropped while state was borrowed");
            }
        }
    }
}