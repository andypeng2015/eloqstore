//! Background write task implementation following C++ background_write.cpp
//!
//! Handles background operations like compaction and archiving

use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;

use crate::types::{TableIdent, PageId, FilePageId, MAX_PAGE_ID, MAX_FILE_PAGE_ID};
use crate::page::{PageMapper, MappingSnapshot};
use crate::storage::AsyncFileManager;
use crate::index::{IndexPageManager, CowRootMeta};
use crate::config::KvOptions;
use crate::Result;
use crate::error::{Error, KvError};

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};
use super::write::WriteTask;

/// Background statistics
#[derive(Debug, Clone)]
pub struct BackgroundStats {
    /// Pages compacted
    pub pages_compacted: usize,
    /// Bytes reclaimed
    pub bytes_reclaimed: usize,
    /// Files compacted
    pub files_compacted: usize,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Background write task for compaction and archiving (following C++ BackgroundWrite)
#[derive(Debug)]
pub struct BackgroundWriteTask {
    /// Table identifier
    table_id: TableIdent,
    /// KV options
    options: Arc<KvOptions>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
    /// COW metadata
    cow_meta: Option<CowRootMeta>,
}

impl BackgroundWriteTask {
    /// Create a new background write task
    pub fn new(
        table_id: TableIdent,
        options: Arc<KvOptions>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            table_id,
            options,
            page_mapper,
            file_manager,
            index_manager,
            cow_meta: None,
        }
    }

    /// Compact data files (following C++ CompactDataFile)
    pub async fn compact_data_file(&mut self) -> Result<BackgroundStats> {
        let start = std::time::Instant::now();
        let mut stats = BackgroundStats {
            pages_compacted: 0,
            bytes_reclaimed: 0,
            files_compacted: 0,
            duration_ms: 0,
        };

        // Only compact in append mode
        if !self.options.data_append_mode {
            return Ok(stats);
        }

        // Find root metadata and check if valid
        {
            let meta = self.index_manager.find_root(&self.table_id)?;
            if meta.root_id == MAX_PAGE_ID {
                return Err(Error::from(KvError::NotFound));
            }
        } // meta dropped here

        // Get current mapping snapshot
        let snapshot = self.page_mapper.snapshot();
        let mapping_count = snapshot.len();

        if mapping_count == 0 {
            return Ok(stats);
        }

        // Check if compaction is needed based on amplification factor
        let space_size = self.calculate_space_size(&snapshot).await?;
        let amplification_factor = space_size as f64 / mapping_count as f64;

        // Using a default amplification factor of 2.0 for now
        if amplification_factor <= 2.0 {
            // No compaction needed
            return Ok(stats);
        }

        // Begin compaction
        self.cow_meta = Some(self.index_manager.make_cow_root(&self.table_id)?);

        // Collect pages that need to be moved
        let pages_to_compact = self.collect_pages_to_compact(&snapshot).await?;

        // Compact pages by copying them to new locations
        for (file_page_id, page_id) in pages_to_compact.iter() {
            // Read the page
            let page_data = self.file_manager
                .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                .await?;

            // Allocate new location
            let new_file_page_id = self.allocate_new_page().await?;

            // Write to new location
            self.file_manager
                .write_page(new_file_page_id.file_id() as u64, new_file_page_id.page_offset(), &page_data)
                .await?;

            // Update mapping
            self.update_mapping(*page_id, new_file_page_id).await?;

            stats.pages_compacted += 1;
        }

        // Calculate reclaimed space
        stats.bytes_reclaimed = (space_size - mapping_count) * self.options.data_page_size;
        stats.files_compacted = self.calculate_files_compacted(space_size, mapping_count);

        // Commit the COW metadata
        if let Some(cow_meta) = &self.cow_meta {
            // Commit would happen here - simplified for now
        }

        stats.duration_ms = start.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Create archive (following C++ CreateArchive)
    pub async fn create_archive(&mut self) -> Result<()> {
        // Archive creation logic would go here
        // This involves moving old data to archive storage
        // For now, this is a placeholder
        Ok(())
    }

    /// Calculate total space size
    async fn calculate_space_size(&self, snapshot: &MappingSnapshot) -> Result<usize> {
        // In append mode, calculate total allocated space
        // This is simplified - real implementation would track file allocations
        Ok(snapshot.len() * 2) // Placeholder multiplier
    }

    /// Collect pages that need compaction
    async fn collect_pages_to_compact(&self, snapshot: &MappingSnapshot) -> Result<Vec<(FilePageId, PageId)>> {
        let mut pages = Vec::new();

        // Iterate through all mapped pages
        for page_id in 0..snapshot.max_page_id() {
            if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
                if file_page_id != MAX_FILE_PAGE_ID {
                    pages.push((file_page_id, page_id));
                }
            }
        }

        // Sort by file ID to compact files in order
        pages.sort_by_key(|(fp, _)| fp.file_id());

        Ok(pages)
    }

    /// Allocate a new page
    async fn allocate_new_page(&self) -> Result<FilePageId> {
        // Simplified allocation - real implementation would use AppendAllocator
        // For now, return a dummy value
        Ok(FilePageId::new(0, 0))
    }

    /// Update page mapping
    async fn update_mapping(&self, page_id: PageId, new_file_page_id: FilePageId) -> Result<()> {
        // Update the mapping in COW metadata
        // This is simplified - real implementation would update cow_meta.mapper
        Ok(())
    }

    /// Calculate number of files compacted
    fn calculate_files_compacted(&self, space_size: usize, mapping_count: usize) -> usize {
        let pages_per_file = self.options.data_page_size / 4096; // Estimate
        ((space_size - mapping_count) / pages_per_file).max(1)
    }
}

#[async_trait]
impl Task for BackgroundWriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::BackgroundWrite
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Low // Background tasks are typically low priority
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // Clone self to get mutable version
        let mut task = BackgroundWriteTask::new(
            self.table_id.clone(),
            self.options.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
            self.index_manager.clone(),
        );

        let stats = task.compact_data_file().await?;
        Ok(TaskResult::Background(Box::new(stats)))
    }

    fn can_merge(&self, _other: &dyn Task) -> bool {
        // Background tasks typically don't merge
        false
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        Err(Error::InvalidState("Background tasks cannot be merged".into()))
    }

    fn estimated_cost(&self) -> usize {
        // High cost for background operations
        1000
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::io::backend::{IoBackendFactory, IoBackendType};

    #[tokio::test]
    async fn test_background_write_task() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();

        let file_manager = Arc::new(AsyncFileManager::new(
            temp_dir.path(),
            4096,
            10,
            backend,
        ));

        let mut options = KvOptions::default();
        options.data_append_mode = true;
        // file_amplify_factor would be set here if it existed
        let options = Arc::new(options);

        let page_mapper = Arc::new(PageMapper::new());
        let index_manager = Arc::new(IndexPageManager::new(file_manager.clone(), options.clone()));

        // Initialize
        file_manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);

        // Create background write task
        let mut task = BackgroundWriteTask::new(
            table_id,
            options,
            page_mapper,
            file_manager,
            index_manager,
        );

        // Execute compaction (will fail due to no data, but should compile)
        let _ = task.compact_data_file().await;
    }
}