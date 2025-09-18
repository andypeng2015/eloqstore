//! io_uring backend implementation (Linux only)
//!
//! IMPORTANT: This backend is NOT thread-safe by design!
//! Each shard must run in its own dedicated thread with its own io_uring instance.
//! This follows the C++ architecture where each shard has its own IouringMgr.
//!
//! The backend can only be used within a tokio_uring runtime, not regular tokio.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;
use crate::error::Error;

use super::{IoBackend, FileHandle, FileMetadata, IoStats, IoBackendType};

/// Wrapper that makes io_uring backend appear thread-safe for type checking
/// but panics if actually used from wrong thread
pub struct UringBackendWrapper {
    /// Thread ID that created this backend
    thread_id: std::thread::ThreadId,
    /// Queue depth
    queue_depth: u32,
}

impl UringBackendWrapper {
    pub fn new(queue_depth: u32) -> Result<Self> {
        Ok(Self {
            thread_id: std::thread::current().id(),
            queue_depth,
        })
    }

    fn check_thread(&self) {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring backend accessed from wrong thread!");
        }
    }
}

// Unsafe impl to satisfy trait bounds
// SAFETY: This is only safe because we check thread ID on every access
unsafe impl Send for UringBackendWrapper {}
unsafe impl Sync for UringBackendWrapper {}

/// File handle wrapper
pub struct UringFileWrapper {
    thread_id: std::thread::ThreadId,
    path: PathBuf,
}

unsafe impl Send for UringFileWrapper {}
unsafe impl Sync for UringFileWrapper {}

#[async_trait]
impl FileHandle for UringFileWrapper {
    async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        // Would need actual implementation with tokio_uring::fs::File
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }

    async fn sync(&self) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }

    async fn sync_data(&self) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }

    async fn file_size(&self) -> Result<u64> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }

    async fn truncate(&self, _size: u64) -> Result<()> {
        Err(Error::NotSupported("Truncate not supported in io_uring".into()))
    }

    async fn allocate(&self, _offset: u64, _len: u64) -> Result<()> {
        if std::thread::current().id() != self.thread_id {
            panic!("io_uring file accessed from wrong thread!");
        }
        Err(Error::NotSupported("io_uring file operations not yet implemented".into()))
    }
}

#[async_trait]
impl IoBackend for UringBackendWrapper {
    fn backend_type(&self) -> IoBackendType {
        IoBackendType::IoUring
    }

    async fn open_file(&self, path: &Path, _create: bool) -> Result<Arc<dyn FileHandle>> {
        self.check_thread();

        // This would need to be implemented with actual tokio_uring operations
        // For now, return a placeholder
        Ok(Arc::new(UringFileWrapper {
            thread_id: self.thread_id,
            path: path.to_path_buf(),
        }))
    }

    async fn delete_file(&self, _path: &Path) -> Result<()> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn file_exists(&self, _path: &Path) -> Result<bool> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn create_dir(&self, _path: &Path) -> Result<()> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn create_dir_all(&self, _path: &Path) -> Result<()> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn read_dir(&self, _path: &Path) -> Result<Vec<String>> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn metadata(&self, _path: &Path) -> Result<FileMetadata> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        self.check_thread();
        Err(Error::NotSupported("io_uring operations not yet implemented".into()))
    }

    fn is_async(&self) -> bool {
        true
    }

    fn stats(&self) -> IoStats {
        self.check_thread();
        IoStats::default()
    }

    async fn shutdown(&self) -> Result<()> {
        self.check_thread();
        Ok(())
    }
}

/// Alias for compatibility
pub type UringBackend = UringBackendWrapper;

// NOTE: Full io_uring implementation would require:
// 1. Each shard running in a dedicated std::thread (not tokio task)
// 2. That thread running a tokio_uring::Runtime
// 3. All I/O operations happening within that runtime
// 4. Careful coordination to ensure no cross-thread access
//
// The C++ implementation achieves this by having each shard thread
// own its IouringMgr and call Submit()/PollComplete() in its work loop.
//
// For now, we provide this wrapper that satisfies the type system
// but would need significant refactoring of the shard execution model
// to actually use tokio_uring effectively.