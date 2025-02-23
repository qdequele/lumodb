use lumodb::{Environment, Database, Transaction};
use std::path::Path;
use tempfile::TempDir;

// Common test setup
fn setup_test_env() -> (TempDir, Environment) {
    let temp_dir = TempDir::new().unwrap();
    let mut env = Environment::new().unwrap();
    env.open(temp_dir.path(), Default::default(), 0644).unwrap();
    (temp_dir, env)
}

#[test]
fn test_database_open_close() {
    let (_dir, env) = setup_test_env();
    let txn = env.begin_txn().unwrap();
    
    // Test opening default database
    let db = Database::open(&txn, None, Default::default()).unwrap();
    assert!(db.flags(&txn).unwrap() == 0);
    
    // Test opening named database
    let db = Database::open(&txn, Some("testdb"), Default::default()).unwrap();
    db.close().unwrap();
}

#[test]
fn test_basic_put_get() {
    let (_dir, env) = setup_test_env();
    let txn = env.begin_txn().unwrap();
    let db = Database::open(&txn, None, Default::default()).unwrap();

    // Test putting and getting data
    let key = b"test_key";
    let value = b"test_value";
    
    db.put(&txn, key, value, Default::default()).unwrap();
    let result = db.get(&txn, key).unwrap();
    
    assert_eq!(result.unwrap(), value);
}

#[test]
fn test_delete_operations() {
    let (_dir, env) = setup_test_env();
    let txn = env.begin_txn().unwrap();
    let db = Database::open(&txn, None, Default::default()).unwrap();

    // Insert and then delete data
    let key = b"delete_key";
    let value = b"delete_value";
    
    db.put(&txn, key, value, Default::default()).unwrap();
    assert!(db.get(&txn, key).unwrap().is_some());
    
    db.del(&txn, key, None).unwrap();
    assert!(db.get(&txn, key).unwrap().is_none());
}

#[test]
fn test_database_stats() {
    let (_dir, env) = setup_test_env();
    let txn = env.begin_txn().unwrap();
    let db = Database::open(&txn, None, Default::default()).unwrap();

    // Get initial stats
    let stats = db.stat(&txn).unwrap();
    assert_eq!(stats.entries, 0);

    // Add some entries and check stats
    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(&txn, key.as_bytes(), value.as_bytes(), Default::default()).unwrap();
    }

    let stats = db.stat(&txn).unwrap();
    assert_eq!(stats.entries, 5);
}

#[test]
fn test_error_handling() {
    let (_dir, env) = setup_test_env();
    let txn = env.begin_txn().unwrap();
    let db = Database::open(&txn, None, Default::default()).unwrap();

    // Test key not found
    let result = db.get(&txn, b"nonexistent_key").unwrap();
    assert!(result.is_none());

    // Test duplicate key with NOOVERWRITE flag
    let key = b"unique_key";
    let value1 = b"value1";
    let value2 = b"value2";
    
    db.put(&txn, key, value1, Default::default()).unwrap();
    let result = db.put(&txn, key, value2, WriteFlags::NOOVERWRITE);
    assert!(matches!(result, Err(Error::KeyExist)));
}

#[test]
fn test_transaction_isolation() {
    let (_dir, env) = setup_test_env();
    
    // Write transaction
    let write_txn = env.begin_txn().unwrap();
    let db = Database::open(&write_txn, None, Default::default()).unwrap();
    db.put(&write_txn, b"key", b"value", Default::default()).unwrap();
    
    // Read transaction shouldn't see uncommitted changes
    let read_txn = env.begin_ro_txn().unwrap();
    let result = db.get(&read_txn, b"key").unwrap();
    assert!(result.is_none());
    
    // After commit, changes should be visible
    write_txn.commit().unwrap();
    let read_txn = env.begin_ro_txn().unwrap();
    let result = db.get(&read_txn, b"key").unwrap();
    assert_eq!(result.unwrap(), b"value");
}