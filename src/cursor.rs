use std::ptr::NonNull;

use crate::constants::{CursorFlags, NodeFlags, PageFlags};
use crate::database::Database;
use crate::error::{Error, Result};
use crate::transaction::Transaction;

#[repr(C)]
pub(crate) struct Page {
    /// Page flags (P_LEAF, P_LEAF2, etc.)
    pub(crate) mp_flags: PageFlags,
    /// Lower bound of free space
    pub(crate) mp_lower: u16,
    /// Dynamic array of node pointers
    pub(crate) mp_ptrs: [NonNull<Node>; 0],
    /// Dynamic array of page data
    pub(crate) mp_data: [u8; 0],
}

impl Page {
    pub fn flags(&self) -> PageFlags {
        self.mp_flags
    }

    pub fn number(&self) -> u64 {
        unsafe { *(self as *const _ as *const u64) }
    }

    pub fn from_number(pgno: u64) -> Self {
        Self {
            mp_flags: PageFlags::empty(),
            mp_lower: 0,
            mp_ptrs: [],
            mp_data: [],
        }
    }

    pub fn level(&self) -> u16 {
        0 // Placeholder - implement actual logic
    }

    pub fn entries(&self) -> usize {
        self.mp_lower as usize / std::mem::size_of::<NonNull<Node>>()
    }

    pub fn get_branch_pgno(&self, index: usize) -> Result<u64> {
        if index >= self.entries() {
            return Err(Error::Invalid);
        }
        // For now return a placeholder - actual implementation would read from page data
        Ok(0)
    }
}

#[repr(C)]
struct Node {
    /// Node flags (F_DUPDATA, F_BIGDATA, etc.)
    mn_flags: NodeFlags,
    /// Key size
    mn_ksize: u32,
    /// Data size
    mn_size: u32,
    /// Dynamic array for key data
    mn_key: [u8; 0],
    /// Dynamic array for node data
    mn_data: [u8; 0],
}

// Remove external C functions and just keep the operation constants
// that we'll use in our pure Rust implementation
const GET_BOTH: u32 = 0x8;
const GET_BOTH_RANGE: u32 = 0x10;

/// Internal cursor state
struct CursorState<'txn> {
    /// Current page number
    page: Option<NonNull<Page>>,
    /// Current position on page
    pos: usize,
    /// Stack of parent pages for current position
    stack: Vec<(NonNull<Page>, usize)>,
    /// Cursor state flags
    flags: CursorFlags,
    /// For sorted-duplicate databases
    xcursor: Option<Box<XCursor<'txn>>>,
}

/// Database cursor
///
/// # Error Recovery
/// The cursor implements automatic recovery for common error scenarios:
///
/// - Transaction validation
/// - Database handle validation
/// - Cursor state validation
/// - Cursor initialization check
/// - Write permission check
/// - Key/value size validation
#[derive(Debug)]
pub struct Cursor<'txn> {
    /// Reference to transaction
    pub(crate) txn: NonNull<Transaction<'txn>>,
    /// Database ID
    pub(crate) dbi: u32,
    /// Current page
    pub(crate) page: Option<NonNull<Page>>,
    /// Position in current page
    pub(crate) pos: usize,
    /// Page stack for traversal
    pub(crate) stack: Vec<(NonNull<Page>, usize)>,
    /// Cursor flags
    pub(crate) flags: CursorFlags,
    /// Reference to database
    pub(crate) db: *const Database<'txn>,
    /// Extended cursor info for DUPSORT
    pub(crate) xcursor: Option<Box<Cursor<'txn>>>,
}

/// Extended cursor for sorted duplicates
pub struct XCursor<'txn> {
    /// Sub-cursor for duplicate data
    cursor: Cursor<'txn>,
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
        unsafe { (*self.page.as_ptr()).mp_flags.contains(PageFlags::LEAF) }
    }

    /// Get child page at given index
    fn get_child(
        &self,
        index: usize,
        get_page: impl Fn(u32) -> Result<NonNull<Page>>,
    ) -> Result<NonNull<Page>> {
        // Validate page pointer
        let page_ref = unsafe { &*self.page.as_ptr() };

        // Check index bounds
        if index >= page_ref.mp_lower as usize {
            return Err(Error::Invalid);
        }

        // Validate node pointer
        let node_ptr = page_ref.mp_ptrs[index].as_ptr();
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
                node.mn_ksize as usize,
            );
            // Ensure we have enough bytes for a u32
            key_slice
                .get(..4)
                .ok_or(Error::BadValSize)?
                .try_into()
                .map_err(|_| Error::BadValSize)?
        };

        // Convert to page number and get the page
        let child_pgno = u32::from_le_bytes(pgno);
        get_page(child_pgno)
    }
}

/// Helper trait for cursor operations
trait CursorResult {
    fn check_initialized(&self) -> Result<()>;
    fn check_writable(&self) -> Result<()>;
    fn validate_key_size(&self, size: usize) -> Result<()>;
}

impl<'txn> CursorResult for Cursor<'txn> {
    fn check_initialized(&self) -> Result<()> {
        if !self.flags.contains(CursorFlags::INITIALIZED) {
            return Err(Error::CursorNotFound);
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        unsafe {
            let txn = self.txn.as_ref();
            if txn.is_readonly() {
                return Err(Error::TxnReadOnly);
            }
        }
        Ok(())
    }

    fn validate_key_size(&self, size: usize) -> Result<()> {
        if size == 0 {
            return Err(Error::BadValSize);
        }
        Ok(())
    }
}

impl<'txn> Cursor<'txn> {
    /// Create a new cursor
    pub(crate) fn new(
        txn: NonNull<Transaction<'txn>>,
        dbi: u32,
        db: *const Database<'txn>,
    ) -> Self {
        Cursor {
            txn,
            dbi,
            page: None,
            pos: 0,
            stack: Vec::new(),
            flags: CursorFlags::empty(),
            db,
            xcursor: None,
        }
    }

    /// Close the cursor
    pub fn close(&mut self) -> Result<()> {
        self.page = None;
        self.pos = 0;
        self.stack.clear();
        self.flags = CursorFlags::empty();
        self.xcursor = None;
        Ok(())
    }

    /// Get the next key/value pair
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // If no current page, start from first page
        if self.page.is_none() {
            return self.first();
        }

        let page = unsafe { self.page.unwrap().as_ref() };

        // If we're at the end of current page
        if self.pos >= page.mp_ptrs.len() {
            // Try to move to next page if this is a branch page
            if page.flags().contains(PageFlags::BRANCH) {
                // Save current position on stack
                self.stack.push((self.page.unwrap(), self.pos));

                // Get child page number from current position
                let node = unsafe { page.mp_ptrs[self.pos].as_ref() };
                let child_pgno = unsafe { *(node.mn_data.as_ptr() as *const u64) };

                // Load child page
                let child_page = unsafe {
                    let txn = self.txn.as_ref();
                    txn.get_page(child_pgno)?
                };

                self.page = Some(child_page);
                self.pos = 0;

                return self.next();
            }

            // If leaf page and stack empty, we're done
            if self.stack.is_empty() {
                return Ok(None);
            }

            // Pop parent page and position from stack
            let (parent_page, parent_pos) = self.stack.pop().unwrap();
            self.page = Some(parent_page);
            self.pos = parent_pos + 1;

            return self.next();
        }

        // Get key/value from current position
        let node = unsafe { page.mp_ptrs[self.pos].as_ref() };
        let key = unsafe {
            Vec::from_raw_parts(
                node.mn_key.as_ptr() as *mut u8,
                node.mn_ksize as usize,
                node.mn_ksize as usize,
            )
        };
        let value = unsafe {
            Vec::from_raw_parts(
                node.mn_data.as_ptr() as *mut u8,
                node.mn_size as usize,
                node.mn_size as usize,
            )
        };

        // Move to next position
        self.pos += 1;

        Ok(Some((key, value)))
    }

    /// Get the previous key/value pair
    pub fn prev(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // Get current page
        let page = match self.page {
            Some(p) => p,
            None => return Ok(None),
        };

        // If at beginning of page
        if self.pos == 0 {
            // If no parent pages in stack, we're at the start
            if self.stack.is_empty() {
                return Ok(None);
            }

            // Pop parent page and position from stack
            let (parent_page, parent_pos) = self.stack.pop().unwrap();
            self.page = Some(parent_page);
            self.pos = parent_pos;

            // Get key/value from current position
            let node = unsafe { parent_page.as_ref().mp_ptrs[self.pos].as_ref() };
            let key = unsafe {
                Vec::from_raw_parts(
                    node.mn_key.as_ptr() as *mut u8,
                    node.mn_ksize as usize,
                    node.mn_ksize as usize,
                )
            };
            let value = unsafe {
                Vec::from_raw_parts(
                    node.mn_data.as_ptr() as *mut u8,
                    node.mn_size as usize,
                    node.mn_size as usize,
                )
            };

            return Ok(Some((key, value)));
        }

        // Move to previous position
        self.pos -= 1;

        // If branch page, traverse to rightmost leaf
        if unsafe { page.as_ref() }.flags().contains(PageFlags::BRANCH) {
            // Get child page number
            let node = unsafe { page.as_ref().mp_ptrs[self.pos].as_ref() };
            let child_pgno = unsafe { *(node.mn_data.as_ptr() as *const u64) };

            // Push current page/pos to stack
            self.stack.push((page, self.pos));

            // Load child page
            let child_page = unsafe {
                let txn = self.txn.as_ref();
                txn.get_page(child_pgno)?
            };

            self.page = Some(child_page);

            // Go to rightmost position
            self.pos = unsafe { child_page.as_ref().mp_ptrs.len() - 1 };

            return self.prev();
        }

        // Get key/value from current position
        let node = unsafe { page.as_ref().mp_ptrs[self.pos].as_ref() };
        let key = unsafe {
            Vec::from_raw_parts(
                node.mn_key.as_ptr() as *mut u8,
                node.mn_ksize as usize,
                node.mn_ksize as usize,
            )
        };
        let value = unsafe {
            Vec::from_raw_parts(
                node.mn_data.as_ptr() as *mut u8,
                node.mn_size as usize,
                node.mn_size as usize,
            )
        };

        Ok(Some((key, value)))
    }

    /// Get the first key/value pair
    pub fn first(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // Clear cursor state
        self.page = None;
        self.pos = 0;
        self.stack.clear();

        // Get root page
        let root_page = unsafe {
            let txn = self.txn.as_ref();
            let db = self.db.as_ref().ok_or(Error::DbNotFound)?;
            let root_pgno = db.root_page()?;
            if root_pgno == 0 {
                return Ok(None); // Empty database
            }
            txn.get_page(root_pgno)?
        };

        self.page = Some(root_page);

        // Traverse to leftmost leaf
        while let Some(page) = self.page {
            if !unsafe { page.as_ref().flags().contains(PageFlags::BRANCH) } {
                break;
            }

            // Get leftmost child page number
            let node = unsafe { page.as_ref().mp_ptrs[0].as_ref() };
            let child_pgno = unsafe { *(node.mn_data.as_ptr() as *const u64) };

            // Push current page/pos to stack
            self.stack.push((page, 0));

            // Load child page
            let child_page = unsafe {
                let txn = self.txn.as_ref();
                txn.get_page(child_pgno)?
            };

            self.page = Some(child_page);
        }

        // Get key/value from current position
        let page = match self.page {
            Some(p) => p,
            None => return Ok(None),
        };

        let node = unsafe { page.as_ref().mp_ptrs[0].as_ref() };
        let key = unsafe {
            Vec::from_raw_parts(
                node.mn_key.as_ptr() as *mut u8,
                node.mn_ksize as usize,
                node.mn_ksize as usize,
            )
        };
        let value = unsafe {
            Vec::from_raw_parts(
                node.mn_data.as_ptr() as *mut u8,
                node.mn_size as usize,
                node.mn_size as usize,
            )
        };

        Ok(Some((key, value)))
    }

    /// Get the last key/value pair
    pub fn last(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // Get root page
        let root_page = unsafe {
            let txn = self.txn.as_ref();
            let db = self.db.as_ref().ok_or(Error::DbNotFound)?;
            let root_pgno = db.root_page()?;
            if root_pgno == 0 {
                return Ok(None);
            }
            txn.get_page(root_pgno)?
        };

        self.page = Some(root_page);
        self.stack.clear();

        // Traverse to rightmost leaf
        while let Some(page) = self.page {
            if !unsafe { page.as_ref().flags().contains(PageFlags::BRANCH) } {
                break;
            }

            // Get rightmost child page number
            let num_ptrs =
                unsafe { page.as_ref().mp_lower as usize / std::mem::size_of::<NonNull<Node>>() };
            let node = unsafe { page.as_ref().mp_ptrs[num_ptrs - 1].as_ref() };
            let child_pgno = unsafe { *(node.mn_data.as_ptr() as *const u64) };

            // Push current page/pos to stack
            self.stack.push((page, num_ptrs - 1));

            // Load child page
            let child_page = unsafe {
                let txn = self.txn.as_ref();
                txn.get_page(child_pgno)?
            };

            self.page = Some(child_page);
        }

        // Get key/value from current position
        let page = match self.page {
            Some(p) => p,
            None => return Ok(None),
        };

        let num_ptrs =
            unsafe { page.as_ref().mp_lower as usize / std::mem::size_of::<NonNull<Node>>() };
        if num_ptrs == 0 {
            return Ok(None);
        }

        let node = unsafe { page.as_ref().mp_ptrs[num_ptrs - 1].as_ref() };
        let key = unsafe {
            Vec::from_raw_parts(
                node.mn_key.as_ptr() as *mut u8,
                node.mn_ksize as usize,
                node.mn_ksize as usize,
            )
        };
        let value = unsafe {
            Vec::from_raw_parts(
                node.mn_data.as_ptr() as *mut u8,
                node.mn_size as usize,
                node.mn_size as usize,
            )
        };

        self.pos = num_ptrs - 1;
        Ok(Some((key, value)))
    }

    /// Set cursor to specific key
    pub fn set(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // For now just return None - actual implementation would search for key
        Ok(None)
    }

    /// Get value for key
    pub fn get(&mut self, key: &[u8], data: Option<&[u8]>) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // First try to set cursor to the key
        match self.set(key)? {
            None => return Ok(None),
            Some((found_key, found_value)) => {
                // For exact matches, check if key matches
                if found_key != key {
                    return Ok(None);
                }

                // If data is provided, need to match that too
                if let Some(data) = data {
                    if found_value != data {
                        return Ok(None);
                    }
                }

                Ok(Some((found_key, found_value)))
            }
        }
    }

    /// Delete current key/value pair
    pub fn del(&mut self, flags: u32) -> Result<()> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
            if txn.is_readonly() {
                return Err(Error::TxnReadOnly);
            }
        }

        // Get current page and position
        let mut page = match self.page {
            Some(p) => p,
            None => return Err(Error::CursorInvalid),
        };

        let node = unsafe {
            match page.as_ref().mp_ptrs.get(self.pos) {
                Some(n) => n.as_ref(),
                None => return Err(Error::CursorInvalid),
            }
        };

        // Handle duplicate values
        if node.mn_flags.contains(NodeFlags::DUPDATA) {
            if let Some(xcursor) = &mut self.xcursor {
                if flags & GET_BOTH != 0 || flags & GET_BOTH_RANGE != 0 {
                    // Delete specific duplicate value
                    xcursor.del(flags)?;
                    return Ok(());
                }
            }
        }

        // Remove node from page
        unsafe {
            let page_mut = page.as_mut();
            page_mut.mp_ptrs[self.pos] = NonNull::dangling();
            page_mut.mp_flags |= PageFlags::DIRTY;
        }

        // Update cursor position
        if self.pos > 0 {
            self.pos -= 1;
        } else if !self.stack.is_empty() {
            // Move to parent page
            let (parent_page, parent_pos) = self.stack.pop().unwrap();
            self.page = Some(parent_page);
            self.pos = parent_pos;
        } else {
            self.page = None;
            self.pos = 0;
        }

        Ok(())
    }

    /// Get number of duplicate values for current key
    pub fn count(&self) -> Result<usize> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // Get current page and position
        let page = match self.page {
            Some(p) => p,
            None => return Ok(0),
        };

        let node = unsafe {
            match page.as_ref().mp_ptrs.get(self.pos) {
                Some(n) => n.as_ref(),
                None => return Ok(0),
            }
        };

        // If node has duplicate data flag, count duplicates
        if node.mn_flags.contains(NodeFlags::DUPDATA) {
            if let Some(xcursor) = &self.xcursor {
                return xcursor.count();
            }
        }

        // No duplicates, just return 1 if we have a valid entry
        Ok(1)
    }

    /// Check if cursor is closed
    pub fn is_closed(&self) -> bool {
        !self.flags.contains(CursorFlags::INITIALIZED)
    }

    /// Get database ID
    pub fn dbi(&self) -> u32 {
        self.dbi
    }

    /// Check if cursor is unused
    pub fn is_unused(&self) -> bool {
        self.is_closed()
    }

    /// Get current key/value pair
    pub(crate) fn get_current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        unsafe {
            let txn = self.txn.as_ref();
            if !txn.is_valid() {
                return Err(Error::TxnInvalid);
            }
        }

        // Get current page and position
        let page = match self.page {
            Some(p) => p,
            None => return Ok(None),
        };

        let node = unsafe {
            match page.as_ref().mp_ptrs.get(self.pos) {
                Some(n) => n.as_ref(),
                None => return Ok(None),
            }
        };

        // Extract key and value
        let key = unsafe {
            Vec::from_raw_parts(
                node.mn_key.as_ptr() as *mut u8,
                node.mn_ksize as usize,
                node.mn_ksize as usize,
            )
        };
        let value = unsafe {
            Vec::from_raw_parts(
                node.mn_data.as_ptr() as *mut u8,
                node.mn_size as usize,
                node.mn_size as usize,
            )
        };

        Ok(Some((key, value)))
    }

    /// Put a key/value pair
    pub fn put(&mut self, key: &[u8], data: &[u8], flags: u32) -> Result<()> {
        if self.is_closed() {
            return Err(Error::CursorClosed);
        }

        // For now just store the key/value pair at current position
        // TODO: Implement actual page manipulation
        self.pos = 0;
        Ok(())
    }

    /// Get current page number
    pub fn get_page(&self) -> Option<u64> {
        self.page.map(|p| unsafe { p.as_ref().number() })
    }
}

impl<'txn> Drop for Cursor<'txn> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Iterator for cursor ranges
pub struct CursorIterator<'txn> {
    cursor: &'txn mut Cursor<'txn>,
    end_key: Vec<u8>,
    finished: bool,
}

impl<'txn> Iterator for CursorIterator<'txn> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.cursor.get_current() {
            Ok(Some((key, data))) => {
                if key > self.end_key {
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

impl<'txn> PartialEq for Cursor<'txn> {
    fn eq(&self, other: &Self) -> bool {
        self.dbi == other.dbi && std::ptr::eq(self.txn.as_ptr(), other.txn.as_ptr())
    }
}
