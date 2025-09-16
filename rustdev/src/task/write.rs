//! Write task implementation using the new I/O abstraction

use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, FileId, TableIdent};
use crate::page::{DataPage, PageCache, Page, DataPageBuilder};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Single write task
#[derive(Clone, Debug)]
pub struct WriteTask {
    /// Key to write
    key: Key,
    /// Value to write
    value: Value,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl WriteTask {
    /// Create a new write task
    pub fn new(
        key: Key,
        value: Value,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            key,
            value,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
        }
    }
}

#[async_trait]
impl Task for WriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::Write
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // For single writes, delegate to batch write
        let batch_task = BatchWriteTask::new(
            vec![(self.key.clone(), self.value.clone())],
            self.table_id.clone(),
            self.page_cache.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
        );

        batch_task.execute(_ctx).await
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Single writes can be merged into batch writes
        other.task_type() == TaskType::BatchWrite
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Merging handled by batch write
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Batch write task for multiple key-value pairs
#[derive(Clone, Debug)]
pub struct BatchWriteTask {
    /// Key-value pairs to write
    entries: Vec<(Key, Value)>,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl BatchWriteTask {
    /// Create a new batch write task
    pub fn new(
        entries: Vec<(Key, Value)>,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            entries,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
        }
    }

    /// Group entries by target page
    async fn group_by_page(&self) -> Result<HashMap<u32, Vec<(Key, Value)>>> {
        let mut page_entries: HashMap<u32, Vec<(Key, Value)>> = HashMap::new();

        // For now, just group all entries into page 0
        // TODO: Implement proper page allocation based on key ranges
        for (key, value) in &self.entries {
            page_entries.entry(0)
                .or_insert_with(Vec::new)
                .push((key.clone(), value.clone()));
        }

        Ok(page_entries)
    }

    /// Write entries to pages
    async fn write_pages(&self, page_entries: HashMap<u32, Vec<(Key, Value)>>) -> Result<usize> {
        let mut total_written = 0;

        for (page_id, entries) in page_entries {
            // Get page mapping from mapper - need to implement proper lookup
            // For now, use a placeholder mapping
            let page_mapping = crate::page::PageMapping {
                page_id,
                file_page_id: crate::types::FilePageId::from_raw(0), // Placeholder
                file_id: 0, // Placeholder
            };

            // Read existing page or create new one
            let mut data_page = if page_mapping.file_page_id.raw() > 0 {
                let page = self.file_manager
                    .read_page(page_mapping.file_id, page_mapping.file_page_id.page_offset())
                    .await?;
                DataPage::from_page(page_id, page)
            } else {
                // New page
                DataPage::new(page_id, 4096) // Using default page size
            };

            // Store first key for cache update
            let first_key = entries.first().map(|(k, _)| k.clone());

            // Add entries to page
            for (key, value) in entries {
                // Check if page has space
                if !data_page.can_fit(&key, &value) {
                    // Need to split page or use overflow
                    // For now, just write what we have and allocate new page
                    break;
                }

                data_page.insert(key.clone(), value)?;
                total_written += 1;
            }

            // Write page back
            self.file_manager
                .write_page(page_mapping.file_id, page_mapping.file_page_id.page_offset(), data_page.as_page())
                .await?;

            // Update cache
            if let Some(key) = first_key {
                self.page_cache.insert(
                    key,
                    Arc::new(data_page),
                ).await;
            }
        }

        Ok(total_written)
    }
}

#[async_trait]
impl Task for BatchWriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::BatchWrite
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // Group entries by target page
        let page_entries = self.group_by_page().await?;

        // Write to pages
        let written = self.write_pages(page_entries).await?;

        Ok(TaskResult::BatchWrite(written))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Batch writes can merge with other writes to same table
        if let Some(other_batch) = other.as_any().downcast_ref::<BatchWriteTask>() {
            self.table_id == other_batch.table_id
        } else if let Some(other_single) = other.as_any().downcast_ref::<WriteTask>() {
            self.table_id == other_single.table_id
        } else {
            false
        }
    }

    fn merge(&mut self, other: Box<dyn Task>) -> Result<()> {
        if let Some(other_batch) = other.as_any().downcast_ref::<BatchWriteTask>() {
            // Merge entries
            self.entries.extend(other_batch.entries.clone());
            Ok(())
        } else if let Some(other_single) = other.as_any().downcast_ref::<WriteTask>() {
            // Add single write
            self.entries.push((other_single.key.clone(), other_single.value.clone()));
            Ok(())
        } else {
            Err(Error::InvalidState("Cannot merge incompatible tasks".into()))
        }
    }

    fn estimated_cost(&self) -> usize {
        self.entries.len()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Delete task
#[derive(Clone, Debug)]
pub struct DeleteTask {
    /// Key to delete
    key: Key,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl DeleteTask {
    /// Create a new delete task
    pub fn new(
        key: Key,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            key,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
        }
    }
}

#[async_trait]
impl Task for DeleteTask {
    fn task_type(&self) -> TaskType {
        TaskType::Delete
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // TODO: Implement proper delete logic with page lookup
        // For now, just return success
        Ok(TaskResult::Delete(true))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Delete tasks for same key can be merged
        if let Some(other_delete) = other.as_any().downcast_ref::<DeleteTask>() {
            self.key == other_delete.key
        } else {
            false
        }
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Nothing to do - same key means same delete
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        1
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
    use crate::page::{PageCacheConfig, PageMapperConfig};

    #[tokio::test]
    async fn test_write_task() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();

        let file_manager = Arc::new(AsyncFileManager::new(
            temp_dir.path(),
            4096,
            10,
            backend,
        ));

        let page_cache = Arc::new(PageCache::new(PageCacheConfig::default()));
        let page_mapper = Arc::new(PageMapper::new(PageMapperConfig::default()));

        // Initialize
        file_manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // Create write task
        let task = WriteTask::new(
            key.clone(),
            value.clone(),
            table_id,
            page_cache.clone(),
            page_mapper.clone(),
            file_manager.clone(),
        );

        // Execute
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::BatchWrite(count) => assert_eq!(count, 1),
            _ => panic!("Unexpected result"),
        }
    }
}