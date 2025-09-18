//! Test example for per-shard io_uring backend
//!
//! This demonstrates how each shard can use its own io_uring instance
//! without thread safety issues.

use eloqstore_rs::io::backend::{IoBackendFactory, IoBackendType, IoBackendConfig, IoBackend};
use eloqstore_rs::Result;
use tempfile::TempDir;

#[cfg(target_os = "linux")]
async fn test_uring_in_dedicated_thread() -> Result<()> {
    use std::thread;

    println!("Testing io_uring backend in dedicated thread...");

    // Spawn a dedicated thread for io_uring operations
    let handle = thread::spawn(|| {
        // Create tokio_uring runtime in this thread
        let runtime = tokio_uring::Runtime::new(&tokio_uring::builder().entries(256))
            .expect("Failed to create io_uring runtime");

        runtime.block_on(async {
            // Create io_uring backend for this shard
            let config = IoBackendConfig {
                backend_type: IoBackendType::IoUring,
                queue_depth: Some(256),
                ..Default::default()
            };

            let backend = IoBackendFactory::create(config)
                .expect("Failed to create io_uring backend");

            // Create temp directory for testing
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let test_file = temp_dir.path().join("test.dat");

            println!("Backend type: {:?}", backend.backend_type());
            println!("Is async: {}", backend.is_async());

            // Test file operations
            let file = backend.open_file(&test_file, true).await
                .expect("Failed to open file");

            // Write data
            let data = b"Hello from per-shard io_uring!";
            let written = file.write_at(0, data).await
                .expect("Failed to write");
            println!("Wrote {} bytes", written);

            // Read data back
            let read_data = file.read_at(0, data.len()).await
                .expect("Failed to read");
            assert_eq!(read_data.as_ref(), data);
            println!("Read back: {:?}", std::str::from_utf8(&read_data).unwrap());

            // Sync to disk
            file.sync().await.expect("Failed to sync");
            println!("Synced to disk");

            // Get file size
            let size = file.file_size().await.expect("Failed to get size");
            println!("File size: {} bytes", size);

            // Test backend stats
            let stats = backend.stats();
            println!("Stats: reads={}, writes={}, bytes_read={}, bytes_written={}",
                     stats.reads, stats.writes, stats.bytes_read, stats.bytes_written);

            // Shutdown backend
            backend.shutdown().await.expect("Failed to shutdown");
            println!("Backend shutdown complete");
        });
    });

    // Wait for thread to complete
    handle.join().expect("Thread panicked");

    println!("io_uring test completed successfully!");
    Ok(())
}

#[cfg(not(target_os = "linux"))]
async fn test_uring_in_dedicated_thread() -> Result<()> {
    println!("io_uring is only supported on Linux");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("Testing per-shard io_uring backend implementation...\n");

    // Test 1: Regular tokio backend (for comparison)
    println!("Test 1: Tokio backend");
    let tokio_config = IoBackendConfig {
        backend_type: IoBackendType::Tokio,
        ..Default::default()
    };

    let tokio_backend = IoBackendFactory::create(tokio_config)?;
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("tokio_test.dat");

    let file = tokio_backend.open_file(&test_file, true).await?;
    file.write_at(0, b"Tokio test").await?;
    file.sync().await?;
    println!("Tokio backend test passed\n");

    // Test 2: io_uring backend in dedicated thread
    println!("Test 2: io_uring backend (per-shard model)");
    test_uring_in_dedicated_thread().await?;

    println!("\nAll tests completed successfully!");
    Ok(())
}