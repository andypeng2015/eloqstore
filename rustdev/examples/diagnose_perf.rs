use eloqstore::api::Store;
use bytes::Bytes;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=== Performance Diagnosis Test ===");

    let path = "/mnt/ramdisk/diagnose_perf";
    let _ = tokio::fs::remove_dir_all(path).await;

    let store = Store::open(path).await?;

    // Test with increasing numbers of operations
    for num_ops in [1, 5, 10, 20, 50, 100] {
        println!("\n--- Testing with {} operations ---", num_ops);

        let mut total_time = std::time::Duration::ZERO;

        for i in 0..num_ops {
            let key = Bytes::from(format!("key_{:04}", i));
            let value = Bytes::from(format!("value_{:04}", i));

            let start = Instant::now();
            store.async_set("test", key.clone(), value).await?;
            let elapsed = start.elapsed();

            total_time += elapsed;

            if i < 5 || elapsed > std::time::Duration::from_millis(100) {
                println!("  Op {}: {:?}", i, elapsed);
            }

            if elapsed > std::time::Duration::from_secs(1) {
                println!("  WARNING: Operation {} took {:?}!", i, elapsed);
            }
        }

        let avg_time = total_time / num_ops as u32;
        println!("  Total: {:?}, Average: {:?}", total_time, avg_time);

        if avg_time > std::time::Duration::from_millis(100) {
            println!("  Performance degradation detected!");
            break;
        }
    }

    Ok(())
}