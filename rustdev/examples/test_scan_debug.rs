//! Debug test for scan functionality

use eloqstore::EloqStore;
use eloqstore::config::KvOptions;
use eloqstore::types::{Key, Value, TableIdent};
use eloqstore::api::request::{WriteEntry, BatchWriteRequest, WriteOp, ScanRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("=== Scan Debug Test ===\n");

    // Create store with simple config
    let data_dir = "/mnt/ramdisk/scan_debug_test";
    std::fs::create_dir_all(data_dir)?;

    let mut options = KvOptions::default();
    options.data_dirs = vec![data_dir.into()];
    options.data_page_size = 4096;

    println!("Creating store with data_dir: {}", data_dir);
    let mut store = EloqStore::new(options)?;

    println!("Starting store...");
    store.start().await?;

    let table_id = TableIdent::new("test_table", 0);

    // Insert test data
    println!("\n1. Inserting test data...");
    {
        let entries = vec![
            WriteEntry {
                key: Key::from("apple"),
                value: Value::from("fruit1"),
                timestamp: 1000,
                op: WriteOp::Upsert,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("banana"),
                value: Value::from("fruit2"),
                timestamp: 1001,
                op: WriteOp::Upsert,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("carrot"),
                value: Value::from("vegetable1"),
                timestamp: 1002,
                op: WriteOp::Upsert,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("date"),
                value: Value::from("fruit3"),
                timestamp: 1003,
                op: WriteOp::Upsert,
                expire_ts: None,
            },
            WriteEntry {
                key: Key::from("eggplant"),
                value: Value::from("vegetable2"),
                timestamp: 1004,
                op: WriteOp::Upsert,
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
        println!("  Write result: success={}", result.success);
    }

    // Test different scan ranges
    println!("\n2. Testing scans:");

    // Full scan
    {
        println!("\n  Full scan (all keys):");
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from(""),
            end_key: None,
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("    Found {} entries", result.entries.len());
        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("      {} => {}", key_str, val_str);
        }
    }

    // Range scan: b to d
    {
        println!("\n  Range scan (b* to d*):");
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from("b"),
            end_key: Some(Key::from("d")),
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("    Found {} entries", result.entries.len());
        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("      {} => {}", key_str, val_str);
        }

        // Verify we only got keys in range
        for (key, _) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            assert!(key_str.as_ref() >= "b" && key_str.as_ref() < "d", "Key {} out of range [b, d)", key_str);
        }
    }

    // Exact prefix scan
    {
        println!("\n  Prefix scan (keys starting with 'c'):");
        let scan_req = ScanRequest {
            table_id: table_id.clone(),
            start_key: Key::from("c"),
            end_key: Some(Key::from("d")),
            limit: Some(10),
            reverse: false,
            timeout: None,
        };

        let result = store.scan(scan_req).await?;
        println!("    Found {} entries", result.entries.len());
        for (key, value) in &result.entries {
            let key_str = String::from_utf8_lossy(key.as_ref());
            let val_str = String::from_utf8_lossy(value.as_ref());
            println!("      {} => {}", key_str, val_str);
            assert!(key_str.starts_with("c"), "Key {} should start with 'c'", key_str);
        }
    }

    println!("\n=== Test Complete ===");
    Ok(())
}