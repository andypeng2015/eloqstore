use eloqstore::api::Store;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Final Stress Test ===");
    
    let path = "/mnt/ramdisk/final_test";
    let _ = tokio::fs::remove_dir_all(path).await;
    
    let store = Store::open(path).await?;
    let mut golden = BTreeMap::new();
    
    let num_keys = 100;
    let operations = 500;
    
    println!("Writing {} keys...", num_keys);
    let start = Instant::now();
    
    // Write keys
    for i in 0..num_keys {
        let key = Bytes::from(format!("k{:04}", i));
        let value = Bytes::from(format!("v{:04}", i));
        golden.insert(key.clone(), value.clone());
        store.async_set("test", key, value).await?;
    }
    
    let write_time = start.elapsed();
    println!("Wrote {} keys in {:?}", num_keys, write_time);
    
    // Update some keys  
    println!("\nUpdating keys...");
    let update_start = Instant::now();
    
    for i in 0..50 {
        let key = Bytes::from(format!("k{:04}", i * 2));
        let value = Bytes::from(format!("updated_{:04}", i));
        golden.insert(key.clone(), value.clone());
        store.async_set("test", key, value).await?;
    }
    
    let update_time = update_start.elapsed();
    println!("Updated 50 keys in {:?}", update_time);
    
    // Delete some keys
    println!("\nDeleting keys...");
    for i in 10..20 {
        let key = Bytes::from(format!("k{:04}", i));
        golden.remove(&key);
        store.async_delete("test", key).await?;
    }
    
    // Verify all keys
    println!("\nVerifying all keys...");
    let verify_start = Instant::now();
    let mut mismatches = 0;
    
    for i in 0..num_keys {
        let key = Bytes::from(format!("k{:04}", i));
        let expected = golden.get(&key).cloned();
        let actual = store.async_get("test", key.clone()).await?;
        
        if expected != actual {
            mismatches += 1;
            eprintln!("Mismatch at k{:04}", i);
        }
    }
    
    let verify_time = verify_start.elapsed();
    println!("Verified {} keys in {:?}", num_keys, verify_time);
    
    if mismatches == 0 {
        println!("\n✅ SUCCESS! All keys matched!");
        println!("Total time: {:?}", start.elapsed());
        println!("Performance:");
        println!("  Writes: {:.0} ops/sec", num_keys as f64 / write_time.as_secs_f64());
        println!("  Updates: {:.0} ops/sec", 50.0 / update_time.as_secs_f64());
        println!("  Reads: {:.0} ops/sec", num_keys as f64 / verify_time.as_secs_f64());
    } else {
        println!("\n❌ FAILED with {} mismatches", mismatches);
        std::process::exit(1);
    }
    
    Ok(())
}
