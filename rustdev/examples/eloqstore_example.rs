//! EloqStore API Usage Example
//!
//! This example demonstrates the core functionality of EloqStore:
//! - Single key set/get/delete operations
//! - Batch update operations
//! - Scan operations
//!
//! Run with:
//! ```bash
//! cargo run --example eloqstore_example
//! ```

use eloqstore::{EloqStore, Result};
use eloqstore::api::request::{BatchWriteRequest, ReadRequest, ScanRequest, WriteEntry, WriteOp};
use eloqstore::config::KvOptions;
use eloqstore::types::{Key, Value, TableIdent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== EloqStore API Example ===\n");

    // Setup test directory
    let test_dir = PathBuf::from("/tmp/eloqstore_example");
    std::fs::remove_dir_all(&test_dir).ok();

    // Configure store with single shard for simplicity
    let mut options = KvOptions::default();
    options.num_threads = 1;
    options.data_dirs = vec![test_dir.clone()];
    options.data_page_size = 4096;

    // Create and start the store
    println!("Starting EloqStore...");
    let mut store = EloqStore::new(options)?;
    store.start().await?;

    let table_id = TableIdent::new("example_table", 0);

    // ========================================
    // 1. Single Key Set Operation
    // ========================================
    println!("\n1. Single Key Set:");
    {
        let key = Key::from("user:1001");
        let value = Value::from("Alice Smith");

        let write_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries: vec![WriteEntry {
                key: key.clone(),
                value: value.clone(),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            }],
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(write_req).await?;
        assert!(result.success, "Write should succeed");
        println!("   ✓ Set key='user:1001', value='Alice Smith'");
    }

    // ========================================
    // 2. Single Key Get Operation
    // ========================================
    println!("\n2. Single Key Get:");
    {
        let key = Key::from("user:1001");
        let expected_value = Value::from("Alice Smith");

        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: key.clone(),
            timeout: None,
        };

        let result = store.read(read_req).await?;
        assert!(result.value.is_some(), "Value should be found");
        assert_eq!(result.value.unwrap(), expected_value, "Value should match");
        println!("   ✓ Got key='user:1001' => value='Alice Smith'");
    }

    // ========================================
    // 3. Batch Update Operations
    // ========================================
    println!("\n3. Batch Update:");
    {
        let entries = vec![
            // Update existing key
            WriteEntry {
                key: Key::from("user:1001"),
                value: Value::from("Alice Johnson"), // Changed last name
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            // Add new keys
            WriteEntry {
                key: Key::from("user:1002"),
                value: Value::from("Bob Williams"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("user:1003"),
                value: Value::from("Charlie Brown"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("product:2001"),
                value: Value::from("Laptop Pro 15"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("product:2002"),
                value: Value::from("Wireless Mouse"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
        ];

        let write_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries,
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(write_req).await?;
        assert!(result.success, "Batch write should succeed");
        println!("   ✓ Updated 1 existing key and added 4 new keys");
    }

    // Verify batch updates
    println!("\n   Verifying batch updates:");
    {
        // Check updated user:1001
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1001"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert_eq!(result.value, Some(Value::from("Alice Johnson")));
        println!("   ✓ user:1001 => 'Alice Johnson' (updated)");

        // Check new user:1002
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1002"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        println!("   DEBUG: user:1002 read result = {:?}", result.value.as_ref().map(|v| String::from_utf8_lossy(v)));
        assert_eq!(result.value, Some(Value::from("Bob Williams")));
        println!("   ✓ user:1002 => 'Bob Williams'");

        // Check user:1003
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1003"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        println!("   DEBUG: user:1003 read result = {:?}", result.value.as_ref().map(|v| String::from_utf8_lossy(v)));
        assert_eq!(result.value, Some(Value::from("Charlie Brown")));
        println!("   ✓ user:1003 => 'Charlie Brown'");

        // Check product:2001
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("product:2001"),
            timeout: None,
        };
        println!("   DEBUG: Reading product:2001...");
        let result = store.read(read_req).await?;
        println!("   DEBUG: product:2001 read result = {:?}", result.value.as_ref().map(|v| String::from_utf8_lossy(v)));
        assert_eq!(result.value, Some(Value::from("Laptop Pro 15")));
        println!("   ✓ product:2001 => 'Laptop Pro 15'");
    }

    // ========================================
    // 4. Scan Operations
    // ========================================
    println!("\n4. Scan Operations:");
    {
        // Scan all user keys (user:*)
        println!("   Scanning keys starting with 'user:'");
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from("user:"),
            end_key: Some(Key::from("user:~")), // ~ ensures we get all user: keys
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("   DEBUG: Scan found {} entries", result.entries.len());
        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("   DEBUG: Scanned entry: {} => {}", key_str, val_str);
        }
        assert_eq!(result.entries.len(), 3, "Should find 3 user entries");

        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("   ✓ Found: {} => {}", key_str, val_str);
            assert!(key_str.starts_with("user:"), "Key should start with 'user:'");
        }

        // Scan product keys
        println!("\n   Scanning keys starting with 'product:'");
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from("product:"),
            end_key: Some(Key::from("product:~")),
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        assert_eq!(result.entries.len(), 2, "Should find 2 product entries");

        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("   ✓ Found: {} => {}", key_str, val_str);
            assert!(key_str.starts_with("product:"), "Key should start with 'product:'");
        }
    }

    // ========================================
    // 5. Delete Operations
    // ========================================
    println!("\n5. Delete Operations:");
    {
        // Delete a single key
        let delete_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries: vec![WriteEntry {
                key: Key::from("user:1002"),
                value: Value::from(""), // Empty value for delete
                op: WriteOp::Delete,
                timestamp: 0,
                expire_ts: None,
            }],
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(delete_req).await?;
        assert!(result.success, "Delete should succeed");
        println!("   ✓ Deleted key 'user:1002'");

        // Verify deletion
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1002"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert!(result.value.is_none(), "Deleted key should not be found");
        println!("   ✓ Verified: 'user:1002' no longer exists");
    }

    // ========================================
    // 6. Mixed Batch Operations
    // ========================================
    println!("\n6. Mixed Batch Operations (Update + Delete):");
    {
        let entries = vec![
            // Update an existing key
            WriteEntry {
                key: Key::from("user:1003"),
                value: Value::from("Charles Brown III"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            // Add a new key
            WriteEntry {
                key: Key::from("user:1004"),
                value: Value::from("Diana Prince"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            },
            // Delete a product
            WriteEntry {
                key: Key::from("product:2002"),
                value: Value::from(""),
                op: WriteOp::Delete,
                timestamp: 0,
                expire_ts: None,
            },
        ];

        let write_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries,
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(write_req).await?;
        assert!(result.success, "Mixed batch should succeed");
        println!("   ✓ Updated 1 key, added 1 key, deleted 1 key");

        // Verify operations
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1003"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert_eq!(result.value, Some(Value::from("Charles Brown III")));
        println!("   ✓ user:1003 updated to 'Charles Brown III'");

        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1004"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert_eq!(result.value, Some(Value::from("Diana Prince")));
        println!("   ✓ user:1004 added as 'Diana Prince'");

        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("product:2002"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert!(result.value.is_none());
        println!("   ✓ product:2002 successfully deleted");
    }

    // ========================================
    // 7. Final State Verification
    // ========================================
    println!("\n7. Final State Verification:");
    {
        // Scan all remaining keys
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from(""),
            end_key: Some(Key::from("~")),
            limit: Some(100),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("   Final database state ({} entries):", result.entries.len());

        let mut user_count = 0;
        let mut product_count = 0;

        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("   - {} => {}", key_str, val_str);

            if key_str.starts_with("user:") {
                user_count += 1;
            } else if key_str.starts_with("product:") {
                product_count += 1;
            }
        }

        // We should have:
        // Users: 1001 (Alice Johnson), 1003 (Charles Brown III), 1004 (Diana Prince) = 3
        // Products: 2001 (Laptop Pro 15) = 1
        assert_eq!(user_count, 3, "Should have 3 users");
        assert_eq!(product_count, 1, "Should have 1 product");
        assert_eq!(result.entries.len(), 4, "Should have 4 total entries");
        println!("\n   ✓ All assertions passed!");
    }

    // ========================================
    // 8. Persistence Test
    // ========================================
    println!("\n8. Persistence Test:");
    {
        println!("   Stopping store...");
        store.stop().await?;

        println!("   Restarting store...");
        let mut options = KvOptions::default();
        options.num_threads = 1;
        options.data_dirs = vec![test_dir.clone()];
        options.data_page_size = 4096;

        let mut store = EloqStore::new(options)?;
        store.start().await?;

        // Verify data persisted
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("user:1001"),
            timeout: None,
        };
        let result = store.read(read_req).await?;
        assert_eq!(result.value, Some(Value::from("Alice Johnson")));
        println!("   ✓ Data persisted correctly after restart");

        store.stop().await?;
    }

    // Cleanup
    std::fs::remove_dir_all(&test_dir).ok();

    println!("\n=== Example completed successfully! ===");
    println!("All operations tested and assertions passed.");

    Ok(())
}