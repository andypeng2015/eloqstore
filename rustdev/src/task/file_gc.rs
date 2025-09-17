//! File garbage collection task implementation following C++ file_gc.cpp
//!
//! Handles cleanup of unused data files and expired archives

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{info, error, debug};

use crate::types::{TableIdent, FileId, FilePageId};
use crate::config::KvOptions;
// use crate::storage::manifest::Manifest; // TODO: Implement manifest parsing
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// GC task for file cleanup
#[derive(Debug, Clone)]
pub struct GcTask {
    /// Table identifier
    table_id: TableIdent,
    /// Mapping timestamp
    mapping_ts: u64,
    /// Maximum file ID
    max_file_id: FileId,
    /// Set of files to retain
    retained_files: HashSet<FileId>,
}

impl GcTask {
    /// Create a new GC task
    pub fn new(
        table_id: TableIdent,
        mapping_ts: u64,
        max_file_id: FileId,
        retained_files: HashSet<FileId>,
    ) -> Self {
        Self {
            table_id,
            mapping_ts,
            max_file_id,
            retained_files,
        }
    }

    /// Check if this is a stop signal
    pub fn is_stop_signal(&self) -> bool {
        self.table_id.table_name.is_empty()
    }

    /// Get retained files
    pub fn retained_files(&self) -> &HashSet<FileId> {
        &self.retained_files
    }
}

/// File garbage collector (following C++ FileGarbageCollector)
pub struct FileGarbageCollector {
    /// KV options
    options: Arc<KvOptions>,
    /// Task sender channel
    task_sender: Option<mpsc::UnboundedSender<GcTask>>,
    /// Worker handles
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl FileGarbageCollector {
    /// Create a new file garbage collector
    pub fn new(options: Arc<KvOptions>) -> Self {
        Self {
            options,
            task_sender: None,
            workers: Vec::new(),
        }
    }

    /// Start the garbage collector with specified number of workers
    pub fn start(&mut self, n_workers: u16) {
        if !self.workers.is_empty() {
            return;
        }

        let (tx, rx) = mpsc::unbounded_channel::<GcTask>();
        self.task_sender = Some(tx.clone());

        // Share receiver among workers using Arc<Mutex>
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        // Start worker tasks
        for _ in 0..n_workers {
            let options = self.options.clone();
            let rx = rx.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let task = {
                        let mut rx = rx.lock().await;
                        rx.recv().await
                    };

                    match task {
                        Some(task) if task.is_stop_signal() => break,
                        Some(task) => {
                            // Use first data directory
                            let dir_path = if !options.data_dirs.is_empty() {
                                options.data_dirs[0].join(format!("table_{}", task.table_id.table_name))
                            } else {
                                PathBuf::from("./data").join(format!("table_{}", task.table_id.table_name))
                            };
                            let _ = FileGarbageCollector::execute(
                                &options,
                                &dir_path,
                                task.mapping_ts,
                                task.max_file_id,
                                task.retained_files,
                            ).await;
                        }
                        None => break,
                    }
                }
            });

            self.workers.push(handle);
        }

        info!("File garbage collector started with {} workers", n_workers);
    }

    /// Stop the garbage collector
    pub async fn stop(&mut self) {
        if self.workers.is_empty() {
            return;
        }

        // Send stop signals to all workers
        if let Some(sender) = &self.task_sender {
            for _ in &self.workers {
                let stop_task = GcTask::new(
                    TableIdent::new("", 0),
                    0,
                    0,
                    HashSet::new(),
                );
                let _ = sender.send(stop_task);
            }
        }

        // Wait for all workers to finish
        for worker in self.workers.drain(..) {
            let _ = worker.await;
        }

        self.task_sender = None;
        info!("File garbage collector stopped");
    }

    /// Add a GC task
    pub fn add_task(
        &self,
        table_id: TableIdent,
        ts: u64,
        max_file_id: FileId,
        retained_files: HashSet<FileId>,
    ) -> bool {
        if let Some(sender) = &self.task_sender {
            sender.send(GcTask::new(table_id, ts, max_file_id, retained_files)).is_ok()
        } else {
            false
        }
    }


    /// Execute garbage collection (following C++ Execute)
    pub async fn execute(
        opts: &KvOptions,
        dir_path: &Path,
        mapping_ts: u64,
        max_file_id: FileId,
        mut retained_files: HashSet<FileId>,
    ) -> Result<()> {
        let mut archives = Vec::new();
        let mut gc_data_files = Vec::new();

        // Scan all archives and data files
        let mut entries = tokio::fs::read_dir(dir_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();

            // Skip temporary files
            if name.ends_with(".tmp") {
                continue;
            }

            // Parse file name to determine type
            if name.starts_with("manifest_") {
                // Archive file
                if let Some(ts_str) = name.strip_prefix("manifest_") {
                    if let Ok(ts) = ts_str.parse::<u64>() {
                        if ts <= mapping_ts {
                            archives.push(ts);
                        }
                    }
                }
            } else if name.starts_with("data_") {
                // Data file
                if let Some(id_str) = name.strip_prefix("data_") {
                    if let Ok(file_id) = id_str.parse::<FileId>() {
                        if file_id < max_file_id {
                            gc_data_files.push(file_id);
                        }
                    }
                }
            }
        }

        // Clear expired archives
        if archives.len() > opts.num_retained_archives {
            archives.sort_by(|a, b| b.cmp(a)); // Sort descending

            while archives.len() > opts.num_retained_archives {
                let ts = archives.pop().unwrap();
                let archive_path = dir_path.join(format!("manifest_{}", ts));

                match tokio::fs::remove_file(&archive_path).await {
                    Ok(_) => {
                        info!("GC removed archive: {:?}", archive_path);
                    }
                    Err(e) => {
                        error!("Failed to remove archive {:?}: {}", archive_path, e);
                    }
                }
            }
        }

        // Get all currently used data files by archives and manifest
        for ts in &archives {
            let archive_path = dir_path.join(format!("manifest_{}", ts));

            // Read archive content
            match tokio::fs::read(&archive_path).await {
                Ok(buffer) => {
                    // Parse manifest and extract retained files
                    // This is simplified - real implementation would parse the manifest
                    Self::extract_retained_files(&buffer, &mut retained_files, opts);
                }
                Err(e) => {
                    error!("Failed to read archive {:?}: {}", archive_path, e);
                    continue;
                }
            }
        }

        // Clear unused data files
        for file_id in gc_data_files {
            if !retained_files.contains(&file_id) {
                let data_path = dir_path.join(format!("data_{}", file_id));

                match tokio::fs::remove_file(&data_path).await {
                    Ok(_) => {
                        info!("GC removed data file: {:?}", data_path);
                    }
                    Err(e) => {
                        error!("Failed to remove data file {:?}: {}", data_path, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Extract retained files from manifest buffer
    fn extract_retained_files(
        buffer: &[u8],
        retained_files: &mut HashSet<FileId>,
        opts: &KvOptions,
    ) {
        // This is a simplified version
        // Real implementation would properly parse the manifest
        // and extract file IDs from the mapping table

        // For now, just mark as TODO
        debug!("TODO: Extract retained files from manifest");
    }
}

/// Get retained files from mapping table (following C++ GetRetainedFiles)
pub fn get_retained_files(
    result: &mut HashSet<FileId>,
    mapping_table: &[u64],
    pages_per_file_shift: u8,
) {
    for &val in mapping_table {
        // Check if this is a file page ID
        // Check if this is a file page ID (simplified check)
        if val != 0 && val != u64::MAX {
            let fp_id = FilePageId::from_raw(val);
            let file_id = (fp_id.file_id() as u64) >> pages_per_file_shift;
            result.insert(file_id);
        }
    }
}

/// File GC task implementation
#[derive(Debug, Clone)]
pub struct FileGcTask {
    /// Table identifier
    table_id: TableIdent,
    /// Options
    options: Arc<KvOptions>,
    /// Mapping timestamp
    mapping_ts: u64,
    /// Max file ID
    max_file_id: FileId,
    /// Retained files
    retained_files: HashSet<FileId>,
}

impl FileGcTask {
    /// Create a new file GC task
    pub fn new(
        table_id: TableIdent,
        options: Arc<KvOptions>,
        mapping_ts: u64,
        max_file_id: FileId,
        retained_files: HashSet<FileId>,
    ) -> Self {
        Self {
            table_id,
            options,
            mapping_ts,
            max_file_id,
            retained_files,
        }
    }
}

#[async_trait]
impl Task for FileGcTask {
    fn task_type(&self) -> TaskType {
        TaskType::FileGC
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Low
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // Use first data directory
        let dir_path = if !self.options.data_dirs.is_empty() {
            self.options.data_dirs[0].join(format!("table_{}", self.table_id.table_name))
        } else {
            PathBuf::from("./data").join(format!("table_{}", self.table_id.table_name))
        };

        FileGarbageCollector::execute(
            &self.options,
            &dir_path,
            self.mapping_ts,
            self.max_file_id,
            self.retained_files.clone(),
        ).await?;

        Ok(TaskResult::FileGC)
    }

    fn can_merge(&self, _other: &dyn Task) -> bool {
        false
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        Err(Error::InvalidState("File GC tasks cannot be merged".into()))
    }

    fn estimated_cost(&self) -> usize {
        100 // File operations are expensive
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_gc() {
        let temp_dir = TempDir::new().unwrap();
        let mut options = KvOptions::default();
        options.data_dirs = vec![temp_dir.path().to_path_buf()];
        options.num_retained_archives = 2;
        let options = Arc::new(options);

        let mut gc = FileGarbageCollector::new(options.clone());

        // Start with 2 workers
        gc.start(2);

        // Add a test task
        let table_id = TableIdent::new("test", 1);
        let retained = HashSet::new();
        gc.add_task(table_id, 100, 10, retained);

        // Stop the GC
        gc.stop().await;
    }

    #[test]
    fn test_get_retained_files() {
        let mut result = HashSet::new();
        let mapping_table = vec![
            0x0000000100000001_u64, // File page ID for file 1
            0x0000000200000002_u64, // File page ID for file 2
        ];

        get_retained_files(&mut result, &mapping_table, 12);

        // Should extract file IDs
        assert!(result.len() > 0);
    }
}