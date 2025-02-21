use std::ptr;
use std::alloc::{self, Layout};
use std::cmp::Ordering;

bitflags::bitflags! {
    /// Flags for IDL operations
    #[derive(Debug, Clone, Copy)]
    pub struct IdlFlags: u32 {
        const FIXED_SIZE = 0b0000_0001;
        const SORTED = 0b0000_0010;
    }
}

/// A generic unsigned ID number
pub type ID = usize;

/// ID List - a sorted array of IDs in descending order.
/// The first element is a counter for how many IDs are in the list.
#[derive(Debug)]
pub struct IDL {
    // Raw pointer to allocated memory: [capacity, count, id1, id2, ...]
    ptr: *mut ID,
    flags: IdlFlags,
}

// Constants from the C version
const IDL_LOGN: usize = 16;
pub const IDL_DB_SIZE: usize = 1 << IDL_LOGN;
pub const IDL_UM_SIZE: usize = 1 << (IDL_LOGN + 1);
pub const IDL_DB_MAX: usize = IDL_DB_SIZE - 1;
pub const IDL_UM_MAX: usize = IDL_UM_SIZE - 1;

impl IDL {
    /// Allocate a new IDL with given capacity
    pub fn new(capacity: usize) -> Option<Self> {
        // Allocate memory for: [capacity, count, elements...]
        let layout = Layout::array::<ID>(capacity + 2).ok()?;
        let ptr = unsafe { alloc::alloc(layout) as *mut ID };
        
        if ptr.is_null() {
            return None;
        }

        unsafe {
            // Store capacity at ptr[-1]
            *ptr = capacity;
            // Initialize count to 0 at ptr[0]
            *ptr.add(1) = 0;
        }

        // Return IDL pointing to the count element
        Some(IDL { 
            ptr: unsafe { ptr.add(1) },
            flags: IdlFlags::empty(),
        })
    }

    /// Get number of elements in the list
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { *self.ptr }
    }

    /// Check if the list is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get capacity of the list
    #[inline]
    pub fn capacity(&self) -> usize {
        unsafe { *self.ptr.sub(1) }
    }

    /// Binary search for an ID
    /// Returns the index where the ID should be inserted
    pub fn search(&self, id: ID) -> usize {
        let len = self.len();
        if len == 0 {
            return 1;
        }

        // Create a safe slice for searching
        let slice = unsafe { std::slice::from_raw_parts(self.ptr.add(1), len) };
        
        match slice.binary_search_by(|probe| probe.cmp(&id).reverse()) {
            Ok(pos) => pos + 1,
            Err(pos) => pos + 1,
        }
    }

    /// Append an ID to the list
    pub fn append(&mut self, id: ID) -> Result<(), ()> {
        // Check if we need to grow
        if self.len() >= self.capacity() {
            return Err(());
        }

        let new_len = self.len() + 1;
        unsafe {
            *self.ptr = new_len;
            *self.ptr.add(new_len) = id;
        }

        self.flags.remove(IdlFlags::SORTED);
        Ok(())
    }

    /// Append a list of IDs
    pub fn append_list(&mut self, other: &IDL) -> Result<(), ()> {
        let new_len = self.len() + other.len();
        if new_len > self.capacity() {
            return Err(());
        }

        unsafe {
            ptr::copy_nonoverlapping(
                other.ptr.add(1),
                self.ptr.add(self.len() + 1),
                other.len()
            );
            *self.ptr = new_len;
        }

        self.flags.remove(IdlFlags::SORTED);
        Ok(())
    }

    /// Sort the ID list in descending order
    pub fn sort(&mut self) {
        if self.len() <= 1 || self.flags.contains(IdlFlags::SORTED) {
            return;
        }

        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                self.ptr.add(1),
                self.len()
            );
            slice.sort_unstable_by(|a, b| b.cmp(a));
        }

        self.flags.insert(IdlFlags::SORTED);
    }

    /// Shrink the IDL to the default size if it has grown larger
    pub fn shrink(&mut self) {
        let len = self.len();
        if len > IDL_UM_MAX {
            unsafe {
                *self.ptr = IDL_UM_MAX;
            }
        }
    }
}

impl Drop for IDL {
    fn drop(&mut self) {
        unsafe {
            let capacity = self.capacity();
            let layout = Layout::array::<ID>(capacity + 2).unwrap();
            alloc::dealloc(self.ptr.sub(1) as *mut u8, layout);
        }
    }
}

// Safe implementation of Clone
impl Clone for IDL {
    fn clone(&self) -> Self {
        let capacity = self.capacity();
        let mut new = IDL::new(capacity).unwrap();
        
        unsafe {
            ptr::copy_nonoverlapping(
                self.ptr,
                new.ptr,
                self.len() + 1
            );
        }
        
        new
    }
}

/// ID2 - An ID/pointer pair
#[repr(C)]
pub struct ID2 {
    pub mid: ID,
    pub mptr: *mut std::ffi::c_void,
}

/// ID2L - A sorted array of ID2s
pub struct ID2L {
    ptr: *mut ID2,
}

impl ID2L {
    pub fn new(capacity: usize) -> Option<Self> {
        let layout = Layout::array::<ID2>(capacity + 1).ok()?;
        let ptr = unsafe { alloc::alloc(layout) as *mut ID2 };
        
        if ptr.is_null() {
            return None;
        }

        unsafe {
            (*ptr).mid = 0; // Initialize count
        }

        Some(ID2L { ptr })
    }

    // Similar methods as IDL, adapted for ID2...
} 