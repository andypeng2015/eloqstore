use eloqstore::api::Store;
use bytes::Bytes;
use std::collections::BTreeMap;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Working Stress Test ===");
    
    let path = "/mnt/ramdisk/working_stress";
    let _ = tokio::fs::remove_dir_all(path).await;
    
    // Create store
    let store = Store::open(path).await?;
    
    let mut golden = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    let num_keys = 100;  // Reduced for faster test
    let num_operations = 1000;  // Reduced for faster test
    
    println!("Phase 1: Populating {} keys...", num_keys);
    
    // Phase 1: Populate initial data
    for i in 0..num_keys {
        let key = Bytes::from(format!("key_{:08}", i));
        let value = Bytes::from(format!("initial_value_{:08}", i));
        
        golden.insert(key.clone(), value.clone());
        store.async_set("test", key, value).await?;
        
        if i % 20 == 0 {
            println!("  Populated {}/{} keys", i, num_keys);
        }
    }
    
    println!("\nPhase 2: Running {} random operations...", num_operations);
    
    let mut puts = 0;
    let mut gets = 0;
    let mut deletes = 0;
    let mut mismatches = 0;
    
    for op_num in 0..num_operations {
        if op_num % 100 == 0 {
            println!("  Progress: {}/{} operations", op_num, num_operations);
        }
        
        let op = rng.gen_range(0..100);
        let key_num = rng.gen_range(0..num_keys);
        let key = Bytes::from(format!("key_{:08}", key_num));
        
        if op < 40 {
            // Put (40%)
            let value = Bytes::from(format!("value_{}_{}", key_num, op_num));
            golden.insert(key.clone(), value.clone());
            store.async_set("test", key, value).await?;
            puts += 1;
        } else if op < 80 {
            // Get (40%)
            let golden_val = golden.get(&key).cloned();
            let store_val = store.async_get("test", key.clone()).await?;
            
            if golden_val != store_val {
                mismatches += 1;
                eprintln!("MISMATCH at op {} key_{:08}: golden has {} bytes, store has {} bytes", 
                    op_num, key_num,
                    golden_val.as_ref().map_or(0, |v| v.len()),
                    store_val.as_ref().map_or(0, |v| v.len()));
                if mismatches > 10 {
                    panic!("Too many mismatches!");
                }
            }
            gets += 1;
        } else if op < 95 {
            // Delete (15%)
            golden.remove(&key);
            store.async_delete("test", key).await?;
            deletes += 1;
        } else {
            // Scan (5%)
            let limit = rng.gen_range(1..=10);
            let results = store.async_scan("test", key.clone(), limit).await?;
            
            // Verify results are in order
            let mut prev = None;
            for (k, _v) in &results {
                if let Some(ref p) = prev {
                    if k <= p {
                        panic!("Scan returned out-of-order results!");
                    }
                }
                prev = Some(k.clone());
            }
        }
    }
    
    println!("\n=== Results ===");
    println!("Operations completed:");
    println!("  Puts: {}", puts);
    println!("  Gets: {}", gets);  
    println!("  Deletes: {}", deletes);
    println!("  Mismatches: {}", mismatches);
    
    // Phase 3: Final validation
    println!("\nPhase 3: Final validation...");
    let mut final_mismatches = 0;
    let mut checked = 0;
    
    for (key, expected_val) in &golden {
        let store_val = store.async_get("test", key.clone()).await?;
        if Some(expected_val.clone()) != store_val {
            final_mismatches += 1;
            if final_mismatches <= 5 {
                eprintln!("Final validation mismatch for {:?}", key);
            }
        }
        checked += 1;
        
        if checked % 100 == 0 {
            println!("  Validated {}/{} keys", checked, golden.len());
        }
    }
    
    if mismatches == 0 && final_mismatches == 0 {
        println!("\n✅ ALL TESTS PASSED!");
        println!("Successfully completed {} operations on {} keys", num_operations, num_keys);
    } else {
        println!("\n❌ Test failed with {} mismatches", mismatches + final_mismatches);
        std::process::exit(1);
    }
    
    Ok(())
}
