//! Read task implementation using the new I/O abstraction
//!
//! This is a refactored version that uses the pluggable I/O backend
//! instead of directly using UringManager.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, PageId};
use crate::page::{DataPage, PageCache};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::index::{IndexPageManager, IndexPageIter};
use crate::config::KvOptions;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Read operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOp {
    /// Exact match
    Exact,
    /// Floor - find last key <= search key
    Floor,
    /// Ceiling - find first key >= search key
    Ceiling,
}

/// Read task for fetching a single key
#[derive(Clone, Debug)]
pub struct ReadTask {
    /// Key to read
    key: Key,
    /// Read operation type
    op: ReadOp,
    /// Table identifier
    table_id: crate::types::TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager using I/O abstraction
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
    /// Options
    options: Arc<KvOptions>,
}

impl ReadTask {
    /// Create a new read task
    pub fn new(
        key: Key,
        table_id: crate::types::TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self::with_op(key, ReadOp::Exact, table_id, page_cache, page_mapper, file_manager, index_manager)
    }

    /// Create a new read task with operation type
    pub fn with_op(
        key: Key,
        op: ReadOp,
        table_id: crate::types::TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            key,
            op,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
            options: Arc::new(KvOptions::default()),
        }
    }

    /// Create a floor read task (find last key <= search key)
    pub fn floor(
        key: Key,
        table_id: crate::types::TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self::with_op(key, ReadOp::Floor, table_id, page_cache, page_mapper, file_manager, index_manager)
    }

    /// Create a ceiling read task (find first key >= search key)
    pub fn ceiling(
        key: Key,
        table_id: crate::types::TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self::with_op(key, ReadOp::Ceiling, table_id, page_cache, page_mapper, file_manager, index_manager)
    }

    /// Find the page containing the key by navigating the index
    async fn find_page(&self) -> Result<Option<Arc<DataPage>>> {
        // First check cache
        if let Some(page) = self.page_cache.get(&self.key).await {
            return Ok(Some(page));
        }

        // Get root metadata for the table
        let root_id = {
            let root_meta = self.index_manager.find_root(&self.table_id)?;
            let id = root_meta.root_id;
            drop(root_meta); // Drop to release raw pointer before await
            id
        };

        if root_id == PageId::MAX {
            // Empty tree
            return Ok(None);
        }

        // Navigate the index tree to find the data page
        let page_id = self.seek_index(root_id).await?;
        if page_id == PageId::MAX {
            return Ok(None);
        }

        // Map logical page_id to physical file_page_id
        let snapshot = self.page_mapper.snapshot();
        let file_page_id = snapshot.to_file_page(page_id)?;

        // Load the page from disk
        let page_data = self.file_manager.read_page(
            file_page_id.file_id() as u64,
            file_page_id.page_offset()
        ).await?;

        // Parse as data page
        let data_page = DataPage::from_page(page_id, page_data);
        let page_arc = Arc::new(data_page);

        // Cache the page
        self.page_cache.insert(self.key.clone(), page_arc.clone()).await;

        Ok(Some(page_arc))
    }

    /// Navigate the index tree to find the data page containing the key
    async fn seek_index(&self, mut curr_page_id: PageId) -> Result<PageId> {
        let mapper = self.page_mapper.snapshot();

        loop {
            // Load the index page
            let idx_page = match self.index_manager.find_page(&mapper, curr_page_id).await {
                Ok(page) => page,
                Err(_) => return Ok(PageId::MAX),
            };
            idx_page.pin();

            // Create iterator and seek to key
            let mut iter = IndexPageIter::new(&idx_page, &self.options);
            iter.seek(self.key.as_ref());

            // Get the page ID for the next level
            let next_page_id = iter.get_page_id();
            idx_page.unpin();

            if next_page_id == PageId::MAX {
                return Ok(PageId::MAX);
            }

            // Check if this index points to data pages
            if idx_page.is_pointing_to_leaf() {
                // Found the data page
                return Ok(next_page_id);
            }

            // Continue down the tree
            curr_page_id = next_page_id;
        }
    }

    /// Find the last key <= search key (floor operation)
    async fn find_floor_key(&self) -> Result<Option<Bytes>> {
        let snapshot = self.page_mapper.snapshot();
        let mut best_match: Option<(Key, Value)> = None;

        // Iterate through pages to find floor
        for page_id in 0..snapshot.max_page_id() {
            if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
                match self.file_manager
                    .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                    .await {
                    Ok(page_data) => {
                        let data_page = DataPage::from_page(page_id, page_data);
                        let mut iter = crate::page::DataPageIterator::new(&data_page);

                        // Use seek_floor to find the last key <= search key
                        if iter.seek_floor(&self.key) {
                            if let Some(value) = iter.value() {
                                best_match = Some((iter.key().unwrap_or_default(), value));
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }

        Ok(best_match.map(|(_, v)| v))
    }

    /// Find the first key >= search key (ceiling operation)
    async fn find_ceiling_key(&self) -> Result<Option<Bytes>> {
        let snapshot = self.page_mapper.snapshot();

        // Iterate through pages to find ceiling
        for page_id in 0..snapshot.max_page_id() {
            if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
                match self.file_manager
                    .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                    .await {
                    Ok(page_data) => {
                        let data_page = DataPage::from_page(page_id, page_data);
                        let mut iter = crate::page::DataPageIterator::new(&data_page);

                        // Use seek to find the first key >= search key
                        if iter.seek(&self.key) {
                            if let Some(value) = iter.value() {
                                return Ok(Some(value));
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl Task for ReadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Read
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // Find the page containing the key
        let page = match self.find_page().await? {
            Some(p) => p,
            None => return Ok(TaskResult::Read(None)),
        };

        // Search for key in page
        let value = page.get(&self.key)?;

        Ok(TaskResult::Read(value.map(Bytes::from)))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Read tasks for the same key can be merged
        if let Some(other_read) = other.as_any().downcast_ref::<ReadTask>() {
            self.key == other_read.key
        } else {
            false
        }
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Nothing to do for read merge - same key means same result
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        // Cost is one page read
        1
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Batch read task for multiple keys
#[derive(Clone, Debug)]
pub struct BatchReadTask {
    /// Keys to read
    keys: Vec<Key>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl BatchReadTask {
    /// Create a new batch read task
    pub fn new(
        keys: Vec<Key>,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            keys,
            page_cache,
            page_mapper,
            file_manager,
        }
    }

    /// Execute reads for all keys
    async fn read_all(&self) -> Result<Vec<Option<Bytes>>> {
        let mut results = Vec::with_capacity(self.keys.len());

        // Group keys by page for efficiency
        let page_keys: std::collections::HashMap<u64, Vec<&Key>> = std::collections::HashMap::new();

        for key in &self.keys {
            // Check cache first
            if let Some(page) = self.page_cache.get(key).await {
                let value = page.get(key)?;
                results.push(value.map(Bytes::from));
                continue;
            }

            // TODO: Implement proper page lookup
            // For now, just mark as not found
            results.push(None);
        }

        // TODO: Implement proper batch page reading
        // For now, pages are not read

        Ok(results)
    }
}

#[async_trait]
impl Task for BatchReadTask {
    fn task_type(&self) -> TaskType {
        TaskType::BatchRead
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        let values = self.read_all().await?;
        Ok(TaskResult::BatchRead(values))
    }

    fn can_merge(&self, _other: &dyn Task) -> bool {
        // Batch reads are not merged
        false
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        Err(Error::InvalidState("Cannot merge batch read tasks".into()))
    }

    fn estimated_cost(&self) -> usize {
        // Estimate based on number of keys
        self.keys.len()
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
    use crate::page::PageCacheConfig;
    use crate::types::TableIdent;

    #[tokio::test]
    async fn test_read_task() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();

        let file_manager = Arc::new(AsyncFileManager::new(
            temp_dir.path(),
            4096,
            10,
            backend,
        ));

        let page_cache = Arc::new(PageCache::new(PageCacheConfig::default()));
        let page_mapper = Arc::new(PageMapper::new());

        // Initialize
        file_manager.init().await.unwrap();

        // Create a read task
        let table_id = TableIdent::new("test", 1);
        let key = Bytes::from("test_key");

        let options = Arc::new(KvOptions::default());
        let index_manager = Arc::new(IndexPageManager::new(
            file_manager.clone(),
            options.clone(),
        ));

        let task = ReadTask::new(
            key.clone(),
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
        );

        // Execute (should return None since no data)
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::Read(None) => {} // Expected
            _ => panic!("Unexpected result"),
        }
    }
}