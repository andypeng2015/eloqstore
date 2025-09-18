//! Simple debug test to check basic read/write functionality

use eloqstore::EloqStore;
use eloqstore::config::KvOptions;
use eloqstore::types::{Key, Value, TableIdent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("=== Simple Debug Test ===\n");

    // Create store with simple config
    let data_dir = "/mnt/ramdisk/simple_debug_test";
    std::fs::create_dir_all(data_dir)?;

    let mut options = KvOptions::default();
    options.data_dirs = vec![data_dir.into()];
    // Use default options which sets appropriate values
    options.data_page_size = 4096;

    println!("Creating store with data_dir: {}", data_dir);
    let mut store = EloqStore::new(options)?;

    println!("Starting store...");
    store.start().await?;

    let table_id = TableIdent::new("test_table", 0);

    // Test 1: Single write
    println!("\n1. Writing single key...");
    {
        let entries = vec![
            eloqstore::api::request::WriteEntry {
                key: Key::from("test_key_1"),
                value: Value::from("test_value_1"),
                timestamp: 1000,
                op: eloqstore::api::request::WriteOp::Upsert,
                expire_ts: None,
            }
        ];

        let write_req = eloqstore::api::request::BatchWriteRequest {
            table_id: table_id.clone(),
            entries,
            sync: true,
            timeout: None,
        };

        println!("  Writing: key='test_key_1', value='test_value_1'");
        let result = store.batch_write(write_req).await?;
        println!("  Write result: success={}", result.success);
    }

    // Test 2: Read back
    println!("\n2. Reading key back...");
    {
        let read_req = eloqstore::api::request::ReadRequest {
            table_id: table_id.clone(),
            key: Key::from("test_key_1"),
            timeout: None,
        };

        println!("  Reading: key='test_key_1'");
        let result = store.read(read_req).await?;

        match result.value {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("  Found: value='{}'", value_str);
                if value_str == "test_value_1" {
                    println!("  ✓ Value matches!");
                } else {
                    println!("  ✗ Value mismatch! Expected 'test_value_1'");
                }
            }
            None => {
                println!("  ✗ Value not found!");
            }
        }
    }

    // Test 3: Multiple writes
    println!("\n3. Writing multiple keys...");
    {
        let mut entries = Vec::new();
        for i in 1..=5 {
            entries.push(eloqstore::api::request::WriteEntry {
                key: Key::from(format!("key_{}", i)),
                value: Value::from(format!("value_{}", i)),
                timestamp: 1000 + i,
                op: eloqstore::api::request::WriteOp::Upsert,
                expire_ts: None,
            });
            println!("  Adding: key='key_{}' => value='value_{}'", i, i);
        }

        let write_req = eloqstore::api::request::BatchWriteRequest {
            table_id: table_id.clone(),
            entries,
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(write_req).await?;
        println!("  Batch write result: success={}", result.success);
    }

    // Test 4: Read multiple
    println!("\n4. Reading multiple keys...");
    for i in 1..=5 {
        let read_req = eloqstore::api::request::ReadRequest {
            table_id: table_id.clone(),
            key: Key::from(format!("key_{}", i)),
            timeout: None,
        };

        let result = store.read(read_req).await?;
        if let Some(value) = &result.value {
            let value_str = String::from_utf8_lossy(value);
            println!("  key_{} => '{}'", i, value_str);
        } else {
            println!("  key_{} => NOT FOUND", i);
        }
    }

    // Test 5: Scan
    println!("\n5. Scanning keys...");
    {
        let scan_req = eloqstore::api::request::ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from("key_1"),
            end_key: Some(Key::from("key_5")),
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("  Found {} entries", result.entries.len());
        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key);
            let value_str = String::from_utf8_lossy(value);
            println!("    {} => {}", key_str, value_str);
        }
    }

    println!("\n=== Test Complete ===");
    Ok(())
}