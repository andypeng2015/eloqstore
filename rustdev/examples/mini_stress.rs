use eloqstore::api::Store;
use bytes::Bytes;
use std::collections::BTreeMap;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Mini Stress Test ===");
    
    let path = "/mnt/ramdisk/mini_stress";
    let _ = tokio::fs::remove_dir_all(path).await;
    
    println!("Creating store...");
    let store = Store::open(path).await?;
    println!("Store created and started");
    
    let mut golden = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    let num_operations = 100;
    let num_keys = 10;
    
    println!("Running {} operations on {} keys", num_operations, num_keys);
    
    let mut puts = 0;
    let mut gets = 0;
    let mut deletes = 0;
    
    for i in 0..num_operations {
        if i % 20 == 0 {
            println!("Progress: {}/{}", i, num_operations);
        }
        
        let op = rng.gen_range(0..100);
        let key_num = rng.gen_range(0..num_keys);
        let key = Bytes::from(format!("key_{:08}", key_num));
        
        if op < 50 {
            // Put
            let value = Bytes::from(format!("value_{:08}_{}", key_num, i));
            println!("  PUT key_{:08}", key_num);
            golden.insert(key.clone(), value.clone());
            store.async_set("test", key, value).await?;
            puts += 1;
        } else if op < 90 {
            // Get
            println!("  GET key_{:08}", key_num);
            let golden_val = golden.get(&key).cloned();
            let store_val = store.async_get("test", key.clone()).await?;
            
            if golden_val != store_val {
                eprintln!("MISMATCH at key_{:08}: golden={:?}, store={:?}", 
                    key_num, golden_val, store_val);
                return Err("Mismatch detected".into());
            }
            gets += 1;
        } else {
            // Delete  
            println!("  DEL key_{:08}", key_num);
            golden.remove(&key);
            store.async_delete("test", key).await?;
            deletes += 1;
        }
    }
    
    println!("\nOperations completed:");
    println!("  Puts: {}", puts);
    println!("  Gets: {}", gets);
    println!("  Deletes: {}", deletes);
    println!("✅ All operations matched!");
    
    Ok(())
}
