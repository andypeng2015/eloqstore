use std::sync::Arc;
use std::path::PathBuf;
use tempfile::TempDir;

use eloqstore_rs::config::KvOptions;
use eloqstore_rs::store::EloqStore;
use eloqstore_rs::api::{ReadRequest, WriteRequest};

#[tokio::test]
async fn test_manifest_persistence() {
    // Create a temporary directory for test data
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Create options with our test directory
    let mut options = KvOptions::default();
    options.data_dirs = vec![data_path.clone()];
    options.num_shards = 1;
    let options = Arc::new(options);

    // Create and start store
    let mut store = EloqStore::new(options.clone());
    store.start().await.expect("Failed to start store");

    // Write some test data
    let write_req = WriteRequest {
        table: "test_table".to_string(),
        key: vec![1, 2, 3],
        value: vec![4, 5, 6],
    };
    
    store.write(write_req).await.expect("Write failed");

    // Stop the store (should save manifest)
    store.stop().await;

    // Create a new store instance with same directory
    let mut store2 = EloqStore::new(options.clone());
    store2.start().await.expect("Failed to start store2");

    // Try to read the data we wrote earlier
    let read_req = ReadRequest {
        table: "test_table".to_string(),
        key: vec![1, 2, 3],
    };

    let result = store2.read(read_req).await;
    assert!(result.is_ok(), "Read after restart failed");
    
    let value = result.unwrap();
    assert_eq!(value, vec![4, 5, 6], "Value mismatch after restart");

    // Stop the second store
    store2.stop().await;

    // Verify manifest file was created
    let manifest_path = data_path.join("shard_0_manifest.bin");
    assert!(manifest_path.exists(), "Manifest file not created");

    println!("Manifest persistence test passed!");
}