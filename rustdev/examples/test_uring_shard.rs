//! Example demonstrating proper per-shard io_uring execution model
//!
//! This shows how each shard would need to run in its own thread
//! with a tokio_uring runtime to properly use io_uring.

use std::thread;
use std::sync::Arc;
use tempfile::TempDir;

/// Simulated shard that runs with io_uring
struct UringShardSimulation {
    id: usize,
    temp_dir: Arc<TempDir>,
}

impl UringShardSimulation {
    fn new(id: usize, temp_dir: Arc<TempDir>) -> Self {
        Self { id, temp_dir }
    }

    /// Run the shard in its dedicated thread with tokio_uring runtime
    fn run(self) {
        println!("Shard {} starting in thread {:?}", self.id, thread::current().id());

        // Create tokio_uring runtime for this shard
        let runtime = match tokio_uring::Runtime::new(&tokio_uring::builder().entries(256)) {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Shard {} failed to create io_uring runtime: {}", self.id, e);
                return;
            }
        };

        // Run the shard's work loop in the tokio_uring runtime
        runtime.block_on(async {
            println!("Shard {} io_uring runtime started", self.id);

            // Test file path for this shard
            let file_path = self.temp_dir.path().join(format!("shard_{}.dat", self.id));

            // Open file using tokio_uring directly
            use tokio_uring::fs::OpenOptions;
            let file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Shard {} failed to open file: {}", self.id, e);
                    return;
                }
            };

            // Write some data
            let data = format!("Data from shard {}", self.id);
            match file.write_at(data.as_bytes().to_vec(), 0).await {
                Ok((_, written)) => {
                    println!("Shard {} wrote {} bytes", self.id, written);
                }
                Err(e) => {
                    eprintln!("Shard {} write failed: {}", self.id, e);
                    return;
                }
            }

            // Read data back
            let buf = vec![0u8; 100];
            match file.read_at(buf, 0).await {
                Ok((buf, read)) => {
                    let data = String::from_utf8_lossy(&buf[..read]);
                    println!("Shard {} read: '{}'", self.id, data);
                }
                Err(e) => {
                    eprintln!("Shard {} read failed: {}", self.id, e);
                    return;
                }
            }

            // Sync to disk
            match file.sync_all().await {
                Ok(_) => println!("Shard {} synced to disk", self.id),
                Err(e) => eprintln!("Shard {} sync failed: {}", self.id, e),
            }

            // Get file stats
            match file.statx().await {
                Ok(stat) => {
                    println!("Shard {} file size: {} bytes", self.id, stat.stx_size);
                }
                Err(e) => {
                    eprintln!("Shard {} stat failed: {}", self.id, e);
                }
            }

            println!("Shard {} completed successfully", self.id);
        });

        println!("Shard {} thread exiting", self.id);
    }
}

fn main() {
    println!("Demonstrating per-shard io_uring execution model\n");

    // Check if we're on Linux
    if !cfg!(target_os = "linux") {
        println!("io_uring is only available on Linux");
        return;
    }

    // Create shared temp directory
    let temp_dir = Arc::new(TempDir::new().expect("Failed to create temp dir"));

    // Number of shards to simulate
    let num_shards = 3;

    // Start each shard in its own thread
    let mut handles = Vec::new();
    for i in 0..num_shards {
        let temp_dir_clone = temp_dir.clone();
        let handle = thread::spawn(move || {
            let shard = UringShardSimulation::new(i, temp_dir_clone);
            shard.run();
        });
        handles.push(handle);
    }

    // Wait for all shards to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(_) => println!("Shard {} thread joined successfully", i),
            Err(_) => eprintln!("Shard {} thread panicked", i),
        }
    }

    println!("\nAll shards completed. This is how EloqStore would need to");
    println!("run shards to properly use io_uring:");
    println!("1. Each shard in a dedicated std::thread");
    println!("2. Each thread running tokio_uring::Runtime");
    println!("3. All I/O operations within that runtime");
    println!("4. No cross-thread access to io_uring objects");
}