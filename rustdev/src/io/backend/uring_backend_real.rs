//! Real io_uring backend implementation (Linux only)
//!
//! This implementation actually uses tokio_uring for file operations.
//! It must run within a tokio_uring runtime, not regular tokio.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;
use crate::error::Error;

use super::{IoBackend, FileHandle, FileMetadata, IoStats, IoBackendType};

/// Thread-local storage for tokio_uring files
/// Since tokio_uring types are !Send, we keep them in thread-local storage
thread_local! {
    static URING_FILES: std::cell::RefCell<std::collections::HashMap<u64, tokio_uring::fs::File>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
    static NEXT_FILE_ID: std::cell::Cell<u64> = std::cell::Cell::new(1);
}

/// Statistics for io_uring backend
pub struct UringStats {
    reads: AtomicU64,
    writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
    total_read_latency: AtomicU64,
    total_write_latency: AtomicU64,
}

impl UringStats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            total_read_latency: AtomicU64::new(0),
            total_write_latency: AtomicU64::new(0),
        }
    }

    fn record_read(&self, bytes: usize, latency_us: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_read_latency.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_write(&self, bytes: usize, latency_us: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_write_latency.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_sync(&self) {
        self.syncs.fetch_add(1, Ordering::Relaxed);
    }

    fn to_io_stats(&self) -> IoStats {
        let reads = self.reads.load(Ordering::Relaxed);
        let writes = self.writes.load(Ordering::Relaxed);

        IoStats {
            reads,
            writes,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            syncs: self.syncs.load(Ordering::Relaxed),
            avg_read_latency_us: if reads > 0 {
                self.total_read_latency.load(Ordering::Relaxed) / reads
            } else {
                0
            },
            avg_write_latency_us: if writes > 0 {
                self.total_write_latency.load(Ordering::Relaxed) / writes
            } else {
                0
            },
        }
    }
}

/// io_uring file handle that uses thread-local storage
pub struct UringFileHandle {
    /// File ID in thread-local storage
    file_id: u64,
    /// Path for debugging
    path: PathBuf,
    /// Thread ID that owns this file
    thread_id: std::thread::ThreadId,
    /// Shared statistics
    stats: Arc<UringStats>,
}

// Mark as Send/Sync with runtime checks
unsafe impl Send for UringFileHandle {}
unsafe impl Sync for UringFileHandle {}

impl Drop for UringFileHandle {
    fn drop(&mut self) {
        if std::thread::current().id() == self.thread_id {
            // Remove from thread-local storage
            URING_FILES.with(|files| {
                files.borrow_mut().remove(&self.file_id);
            });
        }
    }
}

#[async_trait]
impl FileHandle for UringFileHandle {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let start = std::time::Instant::now();

        // Get file from thread-local storage and perform read directly
        let result = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                // Create buffer
                let buf = vec![0u8; len];

                // Clone the file for the async operation
                let file = file.clone();

                // We need to use a different approach since we can't spawn in tokio_uring
                // Return a future that will be awaited outside the with() closure
                Ok((file, buf))
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        let (file, buf) = result;

        // Perform the actual read
        match file.read_at(buf, offset).await {
            Ok((buf, bytes_read)) => {
                let mut result = buf;
                result.truncate(bytes_read);
                let bytes = Bytes::from(result);

                let elapsed = start.elapsed().as_micros() as u64;
                self.stats.record_read(bytes.len(), elapsed);
                Ok(bytes)
            }
            Err(e) => Err(Error::Io(std::io::Error::from(e)))
        }
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let start = std::time::Instant::now();
        let data_vec = data.to_vec();

        // Get file from thread-local storage
        let file = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                Ok(file.clone())
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        // Perform the actual write
        match file.write_at(data_vec, offset).await {
            Ok((_, bytes_written)) => {
                let elapsed = start.elapsed().as_micros() as u64;
                self.stats.record_write(bytes_written, elapsed);
                Ok(bytes_written)
            }
            Err(e) => Err(Error::Io(std::io::Error::from(e)))
        }
    }

    async fn sync(&self) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let file = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                Ok(file.clone())
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        file.sync_all().await.map_err(|e| Error::Io(std::io::Error::from(e)))?;
        self.stats.record_sync();
        Ok(())
    }

    async fn sync_data(&self) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let file = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                Ok(file.clone())
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        file.sync_data().await.map_err(|e| Error::Io(std::io::Error::from(e)))?;
        self.stats.record_sync();
        Ok(())
    }

    async fn file_size(&self) -> Result<u64> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let file = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                Ok(file.clone())
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        match file.statx().await {
            Ok(stat) => Ok(stat.stx_size),
            Err(e) => Err(Error::Io(std::io::Error::from(e)))
        }
    }

    async fn truncate(&self, _size: u64) -> Result<()> {
        // tokio_uring doesn't have direct truncate
        Err(Error::NotSupported("Truncate not supported in tokio_uring".into()))
    }

    async fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::Internal("io_uring file accessed from wrong thread".into()));
        }

        let file = URING_FILES.with(|files| {
            let files = files.borrow();
            if let Some(file) = files.get(&self.file_id) {
                Ok(file.clone())
            } else {
                Err(Error::Internal("File not found in thread-local storage".into()))
            }
        })?;

        file.fallocate(offset, len as i64, 0).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))
    }
}

/// Real io_uring backend implementation
pub struct UringBackendReal {
    /// Thread ID that owns this backend
    thread_id: std::thread::ThreadId,
    /// Statistics
    stats: Arc<UringStats>,
    /// Queue depth
    queue_depth: u32,
}

impl UringBackendReal {
    pub fn new(queue_depth: u32) -> Result<Self> {
        Ok(Self {
            thread_id: std::thread::current().id(),
            stats: Arc::new(UringStats::new()),
            queue_depth,
        })
    }

    fn check_thread(&self) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            Err(Error::Internal("io_uring backend accessed from wrong thread".into()))
        } else {
            Ok(())
        }
    }
}

// Mark as Send/Sync with runtime checks
unsafe impl Send for UringBackendReal {}
unsafe impl Sync for UringBackendReal {}

#[async_trait]
impl IoBackend for UringBackendReal {
    fn backend_type(&self) -> IoBackendType {
        IoBackendType::IoUring
    }

    async fn open_file(&self, path: &Path, create: bool) -> Result<Arc<dyn FileHandle>> {
        self.check_thread()?;

        use tokio_uring::fs::OpenOptions;

        let mut options = OpenOptions::new();
        options.read(true).write(true);

        if create {
            options.create(true);
        }

        // Open the file
        let file = options.open(path).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;

        // Store in thread-local storage
        let file_id = NEXT_FILE_ID.with(|id| {
            let current = id.get();
            id.set(current + 1);
            current
        });

        URING_FILES.with(|files| {
            files.borrow_mut().insert(file_id, file);
        });

        Ok(Arc::new(UringFileHandle {
            file_id,
            path: path.to_path_buf(),
            thread_id: self.thread_id,
            stats: self.stats.clone(),
        }))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        self.check_thread()?;
        tokio_uring::fs::remove_file(path).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))
    }

    async fn file_exists(&self, path: &Path) -> Result<bool> {
        self.check_thread()?;

        use tokio_uring::fs::OpenOptions;
        match OpenOptions::new().read(true).open(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Io(std::io::Error::from(e))),
        }
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        self.check_thread()?;
        tokio_uring::fs::create_dir(path).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.check_thread()?;
        tokio_uring::fs::create_dir_all(path).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        self.check_thread()?;

        // tokio_uring doesn't have read_dir, use blocking
        let path = path.to_path_buf();
        let entries = tokio::task::spawn_blocking(move || {
            let entries = std::fs::read_dir(&path)
                .map_err(|e| Error::Io(e))?;

            let mut names = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| Error::Io(e))?;
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }

            Ok::<Vec<String>, Error>(names)
        }).await.map_err(|e| Error::Internal(format!("Join error: {}", e)))?;

        entries
    }

    async fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        self.check_thread()?;

        use tokio_uring::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .open(path).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;

        let stat = file.statx().await
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;

        Ok(FileMetadata {
            size: stat.stx_size,
            is_dir: (stat.stx_mode & libc::S_IFMT as u16) == libc::S_IFDIR as u16,
            is_file: (stat.stx_mode & libc::S_IFMT as u16) == libc::S_IFREG as u16,
            created: None,
            modified: None,
            accessed: None,
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.check_thread()?;
        tokio_uring::fs::rename(from, to).await
            .map_err(|e| Error::Io(std::io::Error::from(e)))
    }

    fn is_async(&self) -> bool {
        true
    }

    fn stats(&self) -> IoStats {
        self.stats.to_io_stats()
    }

    async fn shutdown(&self) -> Result<()> {
        self.check_thread()?;

        // Clear all files from thread-local storage
        URING_FILES.with(|files| {
            files.borrow_mut().clear();
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_uring_backend_real() {
        // Create tokio_uring runtime
        let runtime = tokio_uring::Runtime::new(&tokio_uring::builder().entries(256))
            .expect("Failed to create io_uring runtime");

        runtime.block_on(async {
            let backend = UringBackendReal::new(256).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let file_path = temp_dir.path().join("test.dat");

            // Test file operations
            let file = backend.open_file(&file_path, true).await.unwrap();

            // Write data
            let data = b"Hello from real io_uring!";
            let written = file.write_at(0, data).await.unwrap();
            assert_eq!(written, data.len());

            // Read data back
            let read_data = file.read_at(0, data.len()).await.unwrap();
            assert_eq!(read_data.as_ref(), data);

            // Sync
            file.sync().await.unwrap();

            // Get file size
            let size = file.file_size().await.unwrap();
            assert_eq!(size, data.len() as u64);

            // Check stats
            let stats = backend.stats();
            assert_eq!(stats.reads, 1);
            assert_eq!(stats.writes, 1);
            assert_eq!(stats.syncs, 1);

            // Shutdown
            backend.shutdown().await.unwrap();
        });
    }
}