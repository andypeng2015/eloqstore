//! Core shard implementation following C++ shard.cpp

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use bytes::Bytes;

use crate::config::KvOptions;
use crate::types::{Key, Value, TableIdent};
use crate::task::{Task, TaskHandle, TaskScheduler};
use crate::page::{PageCache, PageMapper};
use crate::storage::{AsyncFileManager, ManifestData, ManifestFile};
use crate::io::backend::{IoBackendFactory, IoBackendType};
use crate::index::IndexPageManager;
use crate::Result;
use crate::error::{Error, KvError};

/// Shard identifier
pub type ShardId = usize;

/// Request queue item
pub struct RequestItem {
    pub request: Box<dyn crate::store::KvRequest>,
    pub timestamp: Instant,
}

/// Shard state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Shard is initializing
    Initializing,
    /// Shard is running
    Running,
    /// Shard is paused
    Paused,
    /// Shard is stopping
    Stopping,
    /// Shard is stopped
    Stopped,
}

/// Shard statistics
pub struct ShardStats {
    /// Read operations
    pub reads: AtomicU64,
    /// Write operations
    pub writes: AtomicU64,
    /// Scan operations
    pub scans: AtomicU64,
    /// Errors
    pub errors: AtomicU64,
    /// Request latency (microseconds)
    pub latency_us: AtomicU64,
}

impl ShardStats {
    pub fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_us: AtomicU64::new(0),
        }
    }
}

/// Shard implementation (following C++ Shard class)
pub struct Shard {
    /// Shard ID
    id: usize,
    /// KV options
    options: Arc<KvOptions>,
    /// File descriptor limit for this shard
    fd_limit: u32,
    /// Current state
    state: Arc<RwLock<ShardState>>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index page manager
    index_manager: Option<Arc<IndexPageManager>>,
    /// Task scheduler
    scheduler: Arc<TaskScheduler>,
    /// Request queue
    request_queue: mpsc::UnboundedSender<RequestItem>,
    request_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<RequestItem>>>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<ShardStats>,
}

impl Shard {
    /// Create a new shard (following C++ Shard constructor)
    pub fn new(id: usize, options: Arc<KvOptions>, fd_limit: u32) -> Self {
        let page_cache = Arc::new(PageCache::new(Default::default()));
        let page_mapper = Arc::new(PageMapper::new());

        // Create file manager with I/O backend
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();
        let base_dir = if !options.data_dirs.is_empty() {
            options.data_dirs[id % options.data_dirs.len()].clone()
        } else {
            std::path::PathBuf::from(format!("/tmp/shard_{}", id))
        };

        let file_manager = Arc::new(AsyncFileManager::new(
            base_dir,
            options.data_page_size,
            fd_limit as usize,
            backend,
        ));

        let scheduler = Arc::new(TaskScheduler::new(Default::default()));
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            id,
            options: options.clone(),
            fd_limit,
            state: Arc::new(RwLock::new(ShardState::Initializing)),
            page_cache,
            page_mapper,
            file_manager: file_manager.clone(),
            index_manager: None, // Will be initialized in init()
            scheduler,
            request_queue: tx,
            request_rx: Arc::new(RwLock::new(Some(rx))),
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(ShardStats::new()),
        }
    }

    /// Get the manifest file path for this shard
    fn get_manifest_path(&self) -> std::path::PathBuf {
        let base_dir = if !self.options.data_dirs.is_empty() {
            self.options.data_dirs[self.id % self.options.data_dirs.len()].clone()
        } else {
            std::path::PathBuf::from(format!("/tmp/shard_{}", self.id))
        };
        base_dir.join(format!("shard_{}_manifest.bin", self.id))
    }

    /// Initialize shard (following C++ Init)
    pub async fn init(&mut self) -> Result<()> {
        tracing::info!("Initializing shard {}", self.id);

        // Initialize file manager
        self.file_manager.init().await?;

        // Initialize index page manager
        self.index_manager = Some(Arc::new(IndexPageManager::new(
            self.file_manager.clone(),
            self.options.clone(),
        )));

        // Load manifest if exists
        let manifest_path = self.get_manifest_path();
        if manifest_path.exists() {
            tracing::info!("Loading manifest from {:?}", manifest_path);

            match ManifestFile::load_from_file(&manifest_path).await {
                Ok(manifest) => {
                    // Apply manifest to restore state

                    // Restore page mappings
                    for (page_id, file_page_id) in &manifest.mappings {
                        self.page_mapper.update_mapping(*page_id, *file_page_id);
                    }

                    // Restore root metadata for each table
                    if let Some(index_mgr) = &self.index_manager {
                        for (table_id, root_meta) in &manifest.roots {
                            // The index manager needs to be updated to support setting root metadata
                            // For now, log that we would restore it
                            tracing::debug!(
                                "Would restore root for table {}: root={}, ttl_root={}",
                                table_id.table_name, root_meta.root_id, root_meta.ttl_root_id
                            );
                        }
                    }

                    tracing::info!("Manifest loaded successfully with {} mappings", manifest.mappings.len());
                }
                Err(e) => {
                    tracing::warn!("Failed to load manifest: {}. Starting fresh.", e);
                }
            }
        } else {
            tracing::info!("No manifest found at {:?}. Starting fresh.", manifest_path);
        }

        // Restore from checkpoint if available
        // TODO: Implement checkpoint restoration
        // The checkpoint contains:
        // - In-memory index pages
        // - Hot data pages in cache
        // - Transaction state

        // Update state
        let mut state = self.state.write().await;
        *state = ShardState::Running;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!("Shard {} initialized successfully", self.id);

        Ok(())
    }

    /// Add a KV request to the queue (following C++ AddKvRequest)
    pub async fn add_request(&self, request: &dyn crate::store::KvRequest) -> bool {
        if !self.running.load(Ordering::Relaxed) {
            return false;
        }

        // TODO: Properly box/clone the request
        // For now, we need to handle the request ownership properly
        // In real implementation, we'd need to make KvRequest Clone or use Arc

        // Queue the request
        let item = RequestItem {
            request: Box::new(crate::store::ReadRequest::new(
                request.table_id().clone(),
                Bytes::new(),
            )), // Placeholder - need proper request cloning
            timestamp: Instant::now(),
        };

        self.request_queue.send(item).is_ok()
    }

    /// Run the shard worker (following C++ Run coroutine)
    pub async fn run(&self) {
        self.running.store(true, Ordering::Relaxed);
        tracing::info!("Shard {} starting", self.id);

        // Take the receiver
        let mut rx = {
            let mut rx_lock = self.request_rx.write().await;
            rx_lock.take()
        };

        if let Some(mut rx) = rx {
            while self.running.load(Ordering::Relaxed) {
                // Process requests from queue
                tokio::select! {
                    Some(item) = rx.recv() => {
                        // Process the request
                        self.process_request(item).await;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Periodic maintenance tasks
                        self.maintenance().await;
                    }
                }
            }
        }

        tracing::info!("Shard {} stopped", self.id);
    }

    /// Process a single request
    async fn process_request(&self, item: RequestItem) {
        use crate::store::RequestType;
        use crate::task::{ReadTask, BatchWriteTask, TaskContext};

        let start = Instant::now();
        let request = item.request;

        match request.request_type() {
            RequestType::Read => {
                self.stats.reads.fetch_add(1, Ordering::Relaxed);

                if let Some(read_req) = request.as_any().downcast_ref::<crate::store::ReadRequest>() {
                    // Create and execute read task
                    let task = ReadTask::new(
                        read_req.key.clone(),
                        read_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(result) => {
                            // TODO: Set result on request
                            request.set_done(None);
                        }
                        Err(e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            request.set_done(Some(KvError::InternalError));
                        }
                    }
                }
            }
            RequestType::BatchWrite => {
                self.stats.writes.fetch_add(1, Ordering::Relaxed);

                if let Some(write_req) = request.as_any().downcast_ref::<crate::store::BatchWriteRequest>() {
                    // Create and execute batch write task
                    let entries: Vec<_> = write_req.entries.iter()
                        .map(|e| crate::task::write::WriteDataEntry {
                            key: Bytes::from(e.key.clone()),
                            value: Bytes::from(e.value.clone()),
                            op: crate::task::write::WriteOp::Upsert,
                            timestamp: 0,
                            expire_ts: 0,
                        })
                        .collect();

                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            request.set_done(Some(KvError::InternalError));
                            return;
                        }
                    };

                    let task = BatchWriteTask::new(
                        entries,
                        write_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager.clone(),
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(result) => {
                            request.set_done(None);
                        }
                        Err(e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            request.set_done(Some(KvError::InternalError));
                        }
                    }
                }
            }
            RequestType::Scan => {
                self.stats.scans.fetch_add(1, Ordering::Relaxed);

                if let Some(scan_req) = request.as_any().downcast_ref::<crate::store::ScanRequest>() {
                    let index_manager = match self.index_manager.as_ref() {
                        Some(mgr) => mgr.clone(),
                        None => {
                            request.set_done(Some(KvError::InternalError));
                            return;
                        }
                    };

                    let task = crate::task::ScanTask::new(
                        scan_req.begin_key.clone(),
                        Some(scan_req.end_key.clone()),
                        scan_req.max_entries,
                        scan_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                        index_manager,
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(_result) => {
                            // TODO: Set scan results on request
                            request.set_done(None);
                        }
                        Err(_e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            request.set_done(Some(KvError::InternalError));
                        }
                    }
                } else {
                    request.set_done(Some(KvError::InvalidArgument));
                }
            }
            RequestType::Floor => {
                self.stats.reads.fetch_add(1, Ordering::Relaxed);

                if let Some(floor_req) = request.as_any().downcast_ref::<crate::store::ReadRequest>() {
                    let task = ReadTask::floor(
                        floor_req.key.clone(),
                        floor_req.base.table_id.clone(),
                        self.page_cache.clone(),
                        self.page_mapper.clone(),
                        self.file_manager.clone(),
                    );

                    let ctx = TaskContext::default();
                    match task.execute(&ctx).await {
                        Ok(_result) => {
                            // TODO: Set result on request
                            request.set_done(None);
                        }
                        Err(_e) => {
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            request.set_done(Some(KvError::InternalError));
                        }
                    }
                } else {
                    request.set_done(Some(KvError::InvalidArgument));
                }
            }
            _ => {
                // Other request types like Archive, Compact, CleanExpired are background operations
                request.set_done(Some(KvError::NotSupported));
            }
        }

        // Update latency
        let elapsed = start.elapsed().as_micros() as u64;
        self.stats.latency_us.store(elapsed, Ordering::Relaxed);
    }

    /// Periodic maintenance tasks
    async fn maintenance(&self) {
        // Check if maintenance is needed every few iterations
        use std::sync::atomic::AtomicUsize;
        static MAINTENANCE_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let count = MAINTENANCE_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Run maintenance every 1000 iterations
        if count % 1000 == 0 {
            // Evict cold pages from cache if needed
            // TODO: Implement cache eviction based on LRU or similar strategy

            // Trigger background compaction if needed
            if self.options.data_append_mode && self.index_manager.is_some() {
                // TODO: Check amplification factor and trigger compaction
                // For now, just log that we would compact
                tracing::debug!("Would trigger background compaction");
            }

            // Trigger file GC if configured
            if self.options.num_gc_threads > 0 {
                // TODO: Implement file GC triggering
                // Need to get retained files from index and mapping timestamp from manifest
                tracing::debug!("Would trigger file GC");
            }

            // Checkpoint every 10000 iterations (less frequent than other maintenance)
            if count % 10000 == 0 {
                tracing::debug!("Running periodic checkpoint");
                if let Err(e) = self.save_checkpoint().await {
                    tracing::error!("Failed to save periodic checkpoint: {}", e);
                }
            }

            // TODO: Flush dirty pages to disk
        }
    }

    /// Save manifest checkpoint
    async fn save_checkpoint(&self) -> Result<()> {
        let manifest_path = self.get_manifest_path();
        tracing::info!("Saving manifest checkpoint to {:?}", manifest_path);

        // Create manifest data
        let mut manifest = ManifestData::new();
        manifest.version = 1;
        manifest.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Collect page mappings from PageMapper
        // The page mapper would need a method to export all mappings
        // For now, we'll simulate this
        tracing::debug!("Would collect page mappings from PageMapper");

        // Collect root metadata from index manager
        if let Some(index_mgr) = &self.index_manager {
            // The index manager would need a method to export all roots
            // For now, we'll simulate this
            tracing::debug!("Would collect root metadata from IndexPageManager");
        }

        // Save the manifest
        ManifestFile::save_to_file(&manifest, &manifest_path).await?;

        tracing::info!("Manifest checkpoint saved successfully");
        Ok(())
    }

    /// Stop the shard (following C++ Stop)
    pub async fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);

        // Update state
        let mut state = self.state.write().await;
        *state = ShardState::Stopping;
        drop(state); // Release lock before async operations

        // Save manifest checkpoint before shutdown
        if let Err(e) = self.save_checkpoint().await {
            tracing::error!("Failed to save manifest during shutdown: {}", e);
        }

        // Flush any pending operations
        // 1. Flush all dirty pages from page cache
        // The page cache would need a flush method
        tracing::debug!("Would flush page cache");

        // 2. Sync all open files to disk
        if let Err(e) = self.file_manager.sync_all().await {
            tracing::error!("Failed to sync files during shutdown: {}", e);
        }

        // 3. Close all file handles (handled by drop)

        // Update final state
        let mut state = self.state.write().await;
        *state = ShardState::Stopped;

        tracing::info!("Shard {} stopped", self.id);
    }

    /// Get shard statistics
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }

    /// Get shard ID
    pub fn id(&self) -> usize {
        self.id
    }
}