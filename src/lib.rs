// src/lib.rs

/*
LMDB (Lightning Memory-Mapped Database) Architecture Overview

LMDB is a high-performance embedded transactional database that uses memory-mapped files
for persistence. Here's how it works from bottom to top:

Core Concepts:
-------------
1. Memory-Mapped Files
   - The entire database file is mapped into memory using mmap()
   - Changes are written back to disk automatically by the OS's virtual memory system
   - Provides extremely fast read access since no copying is needed
   - Allows zero-copy access to data

2. B+ Tree Structure
   - Data is organized in a B+ tree format
   - Leaf nodes contain actual key-value pairs
   - Interior nodes contain only keys and pointers to child nodes
   - All leaf nodes are at the same level (balanced tree)

3. Copy-On-Write (COW)
   - Modifications don't overwrite existing pages
   - New pages are allocated for changes
   - Ensures ACID compliance and allows concurrent readers
   - Old versions of pages are kept until no longer needed

4. Multi-Version Concurrency Control (MVCC)
   - Readers see a consistent snapshot of the database
   - Writers don't block readers
   - Multiple readers can operate concurrently
   - Only one writer can operate at a time

Key Components:
--------------
1. Environment (MDB_env)
   - Represents a database environment
   - Controls the memory map
   - Manages reader-writer locks
   - Handles configuration (map size, max DBs, etc.)
   - Thread-safe and can be shared between threads

2. Transaction (MDB_txn)
   - Provides ACID properties
   - Can be read-only or read-write
   - Read transactions are very lightweight
   - Write transactions are serialized
   - Child transactions can be nested

3. Database (MDB_dbi)
   - Named B+ trees within the environment
   - Can store different types of data
   - Support various flags (duplicates, integers, etc.)
   - Must be opened within a transaction

4. Cursor (MDB_cursor)
   - Used to traverse and modify data
   - Maintains position in the database
   - Provides ordered access to keys
   - Required for operations on duplicate data

Memory Management:
----------------
1. Page Management
   - Fixed-size pages (typically 4KB)
   - Pages are never split or merged
   - Free pages are tracked in a free list
   - New pages are allocated from the free list or end of file

2. Memory Map
   - Single contiguous memory mapping
   - Size must be set at environment creation
   - Can be resized but requires reopening
   - Should be sized to accommodate expected growth

Concurrency Model:
----------------
1. Reader Lock Table
   - Shared memory segment separate from main database
   - Tracks active readers
   - Used to determine when old pages can be reused
   - Fixed maximum number of simultaneous readers

2. Write Lock
   - Single write transaction at a time
   - Writers don't block readers
   - Readers don't block writers
   - Write locks are implemented using mutexes or semaphores

Implementation Notes for Rust:
---------------------------
1. Safety Considerations
   - Memory mapping requires unsafe code
   - Careful handling of concurrent access
   - Proper synchronization mechanisms
   - Error handling for system calls

2. Key Abstractions
   - Environment should be Send + Sync
   - Transactions should track lifetime of parent env
   - Cursors should be bound to transaction lifetime
   - Clear ownership rules for all components

3. Platform Specifics
   - Different implementations for Windows/POSIX
   - File locking mechanisms vary by OS
   - Memory mapping APIs differ
   - Path handling needs to be platform-aware

Implementation Requirements & Rust-Specific Considerations:
-------------------------------------------------------

1. Core Dependencies
   - memmap2: Safe memory-mapping abstraction
     * Replaces raw mmap() calls
     * Provides RAII-style cleanup
     * Platform-independent API

   - parking_lot: Modern synchronization primitives
     * More efficient than std mutexes
     * Better deadlock detection
     * RwLock for reader-writer synchronization

   - bytemuck: Safe type casting
     * Zero-copy casting between types
     * Useful for page data manipulation
     * Safe handling of aligned memory

2. Key Data Structures
   struct Environment {
       map: memmap2::MmapMut,
       readers: Arc<RwLock<ReaderTable>>,
       write_lock: Mutex<()>,
       meta_pages: [MetaPage; 2],
       max_readers: u32,
       max_dbs: u32,
       // ...
   }

   struct Transaction<'env> {
       env: &'env Environment,
       parent: Option<&'env Transaction<'env>>,
       read_only: bool,
       root_page: PageNum,
       // ...
   }

   struct Database {
       name: String,
       flags: DatabaseFlags,
       root_page: PageNum,
       // ...
   }

3. Memory Layout
   - Page Size: Usually 4KB, must be power of 2
   - Meta Pages: First 2 pages of file
   - Free List: Tracks unused pages
   - B+ Tree Nodes: Rest of pages

   struct Page {
       header: PageHeader,
       data: [u8],  // Variable size based on page type
   }

   struct MetaPage {
       magic: u32,
       version: u32,
       last_page: PageNum,
       root_dbi: PageNum,
       // ...
   }

4. Safety & Synchronization
   - Use RwLock for reader table access
   - Single writer mutex for write transactions
   - Memory barriers for page updates
   - Atomic operations for transaction IDs

5. Platform Considerations
   #[cfg(windows)]
   mod windows {
       // Windows-specific implementations
   }

   #[cfg(unix)]
   mod unix {
       // Unix-specific implementations
   }

6. Error Handling
   enum Error {
       IoError(std::io::Error),
       MapError(memmap2::Error),
       KeyTooLarge,
       DatabaseFull,
       // ...
   }

7. Testing Strategy
   - Unit tests for each component
   - Integration tests for transactions
   - Concurrent access tests
   - Crash recovery tests
   - Platform-specific tests

Simplifications from Original LMDB:
--------------------------------
1. Use Rust's type system:
   - Replace void pointers with generics
   - Use enums for flags and options
   - Strong typing for page types

2. Memory Safety:
   - Replace manual memory management with RAII
   - Use Arc/Rc for shared ownership
   - Leverage Rust's lifetime system

3. Error Handling:
   - Replace error codes with Result type
   - Use custom error types
   - Better error messages

4. Platform Abstraction:
   - Use std::fs for file operations
   - memmap2 for memory mapping
   - parking_lot for synchronization

5. Modern Features:
   - Async support (optional)
   - Iterator implementations
   - Serde integration
   - Builder pattern for configuration

Implementation Order:
------------------
1. Basic Structures:
   - Environment setup
   - Page management
   - Meta page handling

2. Core Operations:
   - B+ tree implementation
   - Key-value operations
   - Page allocation

3. Transactions:
   - Read transaction support
   - Write transaction handling
   - MVCC implementation

4. Concurrency:
   - Reader lock table
   - Writer lock mechanism
   - Transaction isolation

5. Advanced Features:
   - Multiple databases
   - Nested transactions
   - Database flags/options
*/

mod constants;
mod cursor;
mod database;
mod env;
mod error;
mod meta;
mod midl;
mod transaction;
mod types;
mod value;

// Check Cursor -> Database -> Transaction -> Environment

// Module structure
mod btree; // B+ tree implementation
mod cursor; // Cursor implementation
mod db; // Database operations
mod env; // Environment management
mod error; // Error types
mod page; // Page structures and operations
mod platform;
mod txn; // Transaction handling // Platform-specific code

// Public API
pub use cursor::Cursor;
pub use db::Database;
pub use env::Environment;
pub use error::Error;
pub use txn::Transaction;

// Re-export common types
pub type Result<T> = std::result::Result<T, Error>;
