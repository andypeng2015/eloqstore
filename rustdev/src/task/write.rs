//! Write task implementation following C++ batch_write_task.cpp
//!
//! This implementation follows the C++ write path with:
//! - Index tree navigation using stack
//! - Leaf triple page management for linked list updates
//! - Proper page allocation and COW semantics

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent, PageId, MAX_PAGE_ID};
use crate::page::{DataPage, PageCache, DataPageBuilder, DataPageIterator, Page};
use crate::page::PageMapper;
use crate::storage::{AsyncFileManager, ManifestBuilder};
use crate::index::{IndexPageManager, IndexPageBuilder, CowRootMeta, IndexPageIter};
use crate::config::KvOptions;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};
use super::file_gc::FileGarbageCollector;

/// Write operation type
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    /// Insert or update
    Upsert,
    /// Delete key
    Delete,
}

/// Index operation for tracking changes to index pages
#[derive(Debug, Clone)]
struct IndexOp {
    key: Bytes,
    page_id: PageId,
    op: WriteOp,
}

/// Write data entry (following C++ WriteDataEntry)
#[derive(Clone, Debug)]
pub struct WriteDataEntry {
    /// Key to write
    pub key: Key,
    /// Value to write (empty for deletes)
    pub value: Value,
    /// Operation type
    pub op: WriteOp,
    /// Timestamp
    pub timestamp: u64,
    /// TTL expiration timestamp (0 for no expiration)
    pub expire_ts: u64,
}

/// Index stack entry (following C++ IndexStackEntry)
struct IndexStackEntry {
    /// Index page (if any)
    idx_page: Option<Arc<crate::index::MemIndexPage>>,
    /// Index page iterator
    idx_page_iter: crate::index::IndexPageIter<'static>,
    /// KV options
    options: Arc<crate::config::KvOptions>,
    /// Is this a leaf index
    is_leaf_index: bool,
    /// Changes to apply at this level
    changes: Vec<IndexOp>,
}

impl IndexStackEntry {
    /// Create a new stack entry
    fn new(idx_page: Option<Arc<crate::index::MemIndexPage>>, options: Arc<crate::config::KvOptions>) -> Self {
        // Create a dummy iterator for now - will be properly initialized when used
        let dummy_iter = unsafe {
            std::mem::transmute::<crate::index::IndexPageIter<'_>, crate::index::IndexPageIter<'static>>(
                crate::index::index_page_iter::IndexPageIter::new(&[], options.comparator())
            )
        };

        Self {
            idx_page,
            idx_page_iter: dummy_iter,
            options,
            is_leaf_index: false,
            changes: Vec::new(),
        }
    }
}


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
    /// Index manager
    index_manager: Arc<IndexPageManager>,
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
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            key,
            value,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
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
        let entry = WriteDataEntry {
            key: self.key.clone(),
            value: self.value.clone(),
            op: WriteOp::Upsert,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            expire_ts: 0,
        };

        let batch_task = BatchWriteTask::new(
            vec![entry],
            self.table_id.clone(),
            self.page_cache.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
            self.index_manager.clone(),
            None,  // file_gc not available in single write task
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

/// Batch write task for multiple key-value pairs (following C++ BatchWriteTask)
pub struct BatchWriteTask {
    /// Write entries
    entries: Vec<WriteDataEntry>,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
    /// Index stack for tree traversal
    stack: Vec<IndexStackEntry>,
    /// Leaf triple for linked list management
    /// [0]: previous page, [1]: current page, [2]: next page
    leaf_triple: [Option<DataPage>; 3],
    /// Currently applying page
    applying_page: Option<DataPage>,
    /// COW metadata
    cow_meta: Option<CowRootMeta>,
    /// TTL batch for expired entries
    ttl_batch: Vec<WriteDataEntry>,
    /// Data page builder
    data_page_builder: DataPageBuilder,
    /// Data page builder for index pages (reuse DataPageBuilder)
    idx_page_builder: IndexPageBuilder,
    /// Options
    options: Arc<KvOptions>,
    /// Manifest builder for tracking page mappings (following C++ wal_builder_)
    manifest_builder: ManifestBuilder,
    /// Overflow pointers buffer (following C++ overflow_ptrs_)
    overflow_ptrs: Vec<u8>,
    /// File garbage collector reference (optional)
    file_gc: Option<Arc<tokio::sync::Mutex<FileGarbageCollector>>>,
}

impl BatchWriteTask {
    /// Abort the write task and clean up resources (following C++ Abort)
    fn abort(&mut self) {
        // Unpin all index pages in the stack
        while let Some(mut entry) = self.stack.pop() {
            if let Some(ref idx_page) = entry.idx_page {
                idx_page.unpin();
            }
        }

        // Clear leaf triple pages
        for page in &mut self.leaf_triple {
            *page = None;
        }

        // Clear applying page
        self.applying_page = None;
    }

    /// Create a new batch write task
    pub fn new(
        entries: Vec<WriteDataEntry>,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
        file_gc: Option<Arc<tokio::sync::Mutex<FileGarbageCollector>>>,
    ) -> Self {
        Self {
            entries,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
            stack: Vec::new(),
            leaf_triple: [None, None, None],
            applying_page: None,
            cow_meta: None,
            ttl_batch: Vec::new(),
            data_page_builder: DataPageBuilder::new(4096), // Default page size
            idx_page_builder: IndexPageBuilder::new(Arc::new(KvOptions::default())),
            options: Arc::new(KvOptions::default()),
            manifest_builder: ManifestBuilder::new(),
            overflow_ptrs: Vec::new(),
            file_gc,
        }
    }

    /// Seek to position in index stack (following C++ SeekStack)
    async fn seek_stack(&mut self, search_key: &Key) -> Result<()> {
        let comparator = self.index_manager.get_comparator();

        // Navigate up the stack to find the right position
        while self.stack.len() > 1 {
            let stack_entry = self.stack.last_mut().unwrap();

            // Check if we need to move to next sibling or pop
            // Following C++ implementation for bounds checking
            if !stack_entry.idx_page_iter.has_next() {
                self.pop_stack().await?;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Pop from index stack (following C++ Pop)
    async fn pop_stack(&mut self) -> Result<Option<PageId>> {
        tracing::debug!("pop_stack: starting, stack size={}", self.stack.len());

        if self.stack.is_empty() {
            return Ok(None);
        }

        let stack_entry = self.stack.pop().unwrap();

        // If no changes, just return
        if stack_entry.changes.is_empty() {
            let page_id = stack_entry.idx_page.as_ref().map(|p| p.get_page_id());
            tracing::debug!("pop_stack: no changes, returning existing page_id");
            // Unpin the index page if we have one
            if let Some(ref idx_page) = stack_entry.idx_page {
                idx_page.unpin();
            }
            return Ok(page_id);
        }

        tracing::debug!("pop_stack: {} changes to process", stack_entry.changes.len());

        let changes = stack_entry.changes;
        let stack_page = stack_entry.idx_page.as_ref();
        let is_leaf_index = stack_entry.is_leaf_index;

        // Reset index page builder for merging
        self.idx_page_builder.reset();

        // Add leftmost pointer if we have a base page
        if let Some(page) = stack_page {
            let iter = crate::index::index_page_iter::IndexPageIter::new(page.page_ptr(), stack_entry.options.comparator());
            // The first entry is the leftmost pointer with empty key
            self.idx_page_builder.add(b"", iter.get_page_id(), is_leaf_index);
        }

        // Initialize base page iterator if we have a page
        let mut base_page_iter = if let Some(page) = stack_page {
            let mut iter = crate::index::index_page_iter::IndexPageIter::new(page.page_ptr(), stack_entry.options.comparator());
            // The iterator starts at the leftmost pointer which has empty key
            // We need to advance to the first real entry
            let first_valid = iter.next();
            Some(iter)
        } else {
            None
        };

        // Check if we have a valid entry
        let mut is_base_iter_valid = base_page_iter.is_some();

        // Keep track of previous page for redistribution
        let mut prev_page_id = stack_page.map_or(MAX_PAGE_ID, |p| p.get_page_id());
        let mut prev_page_data: Option<Vec<u8>> = None;
        let mut prev_page_key = String::new();

        // Get page key from parent if not root
        let page_key = if !self.stack.is_empty() {
            if let Some(parent) = self.stack.last() {
                String::from_utf8_lossy(parent.idx_page_iter.key().unwrap_or(b"")).to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let mut curr_page_key = page_key.clone();

        // Check if base iterator has real entries (beyond leftmost pointer)
        let base_has_entries = if let Some(iter) = &base_page_iter {
            // Check if we found a valid first entry (which would be after the leftmost ptr)
            iter.key().is_some() && !iter.key().unwrap().is_empty()
        } else {
            false
        };

        // Special case: if we have exactly one change and the base page has no real entries
        // (only leftmost pointer), then we should replace the leftmost pointer
        let single_change_replaces_all = changes.len() == 1 && !base_has_entries;


        // Merge changes with existing entries
        let mut change_iter = changes.into_iter();
        let mut curr_change = change_iter.next();

        // Handle special case: single change replaces entire index
        if single_change_replaces_all {
            if let Some(change) = &curr_change {
                if change.op == WriteOp::Upsert {
                    // Reset builder and set new leftmost pointer
                    self.idx_page_builder.reset();
                    self.idx_page_builder.add(b"", change.page_id, is_leaf_index);
                    // Skip normal merge processing
                    curr_change = None;
                    is_base_iter_valid = false;
                }
            }
        }

        // Process both iterators in merge order
        while is_base_iter_valid && curr_change.is_some() {
            let base_iter = base_page_iter.as_ref().unwrap();
            // Get the key, if None then advance iterator
            let base_key = match base_iter.key() {
                Some(key) => key,
                None => {
                    // No valid key, skip this entry
                    if let Some(ref mut iter) = base_page_iter {
                        is_base_iter_valid = iter.next();
                    }
                    continue;
                }
            };
            let base_page_id = base_iter.get_page_id();

            let change = curr_change.as_ref().unwrap();
            let change_key = &change.key;
            let change_page_id = change.page_id;

            // Compare keys
            let cmp_result = base_key.cmp(change_key.as_ref());

            let (new_key, new_page_id, advance_base, advance_change) = match cmp_result {
                std::cmp::Ordering::Less => {
                    // Base key comes first
                    (base_key.to_vec(), base_page_id, true, false)
                }
                std::cmp::Ordering::Equal => {
                    // Keys are equal, apply change
                    match change.op {
                        WriteOp::Delete => {
                            // Skip this entry
                            (vec![], MAX_PAGE_ID, true, true)
                        }
                        WriteOp::Upsert => {
                            // Replace with new page
                            (change_key.to_vec(), change_page_id, true, true)
                        }
                    }
                }
                std::cmp::Ordering::Greater => {
                    // Change key comes first (must be insert)
                    debug_assert!(matches!(change.op, WriteOp::Upsert));
                    (change_key.to_vec(), change_page_id, false, true)
                }
            };

            // Add to page if not deleted (skip if key is empty - that shouldn't happen)
            if !new_key.is_empty() && new_page_id != MAX_PAGE_ID {
                // Check if page is full
                if !self.idx_page_builder.add(&new_key, new_page_id, is_leaf_index) {
                    // Page is full, finish current page
                    if let Some(page_data) = prev_page_data.take() {
                        // Flush previous page
                        self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                    }

                    prev_page_key = curr_page_key.clone();
                    prev_page_data = Some(self.idx_page_builder.finish().to_vec());
                    // COW: Always allocate new page for index pages
                    let old_prev = prev_page_id;
                    prev_page_id = self.page_mapper.allocate_page()?;
                    if old_prev != MAX_PAGE_ID {
                        // TODO: Add to GC list instead of immediate free
                        // self.free_page(old_prev);
                    }

                    curr_page_key = String::from_utf8_lossy(&new_key).to_string();
                    self.idx_page_builder.reset();

                    // Add the entry that failed to the new page (must succeed)
                    tracing::debug!("Adding entry to new index page after split: key_len={}, page_id={}, is_leaf={}",
                                  new_key.len(), new_page_id, is_leaf_index);
                    let success = self.idx_page_builder.add(&new_key, new_page_id, is_leaf_index);
                    assert!(success, "Failed to add entry to empty index page");
                }
            }

            // Advance iterators
            if advance_base {
                if let Some(ref mut iter) = base_page_iter {
                    is_base_iter_valid = iter.next();
                }
            }
            if advance_change {
                curr_change = change_iter.next();
            }
        }

        // Process remaining base entries
        while is_base_iter_valid {
            if let Some(ref base_iter) = base_page_iter {
                // Skip entries with no key (shouldn't happen in valid index)
                if let Some(new_key) = base_iter.key() {
                    // Skip empty keys (leftmost pointer was already added)
                    if new_key.is_empty() {
                        tracing::debug!("Skipping empty key in base iterator");
                        if let Some(ref mut iter) = base_page_iter {
                            is_base_iter_valid = iter.next();
                        }
                        continue;
                    }

                    let new_page_id = base_iter.get_page_id();

                    // Check if page is full
                    if !self.idx_page_builder.add(new_key, new_page_id, is_leaf_index) {
                        // Page is full, finish current page
                        if let Some(page_data) = prev_page_data.take() {
                            // Flush previous page
                            self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                        }

                        prev_page_key = curr_page_key.clone();
                        prev_page_data = Some(self.idx_page_builder.finish().to_vec());
                        prev_page_id = self.page_mapper.allocate_page()?;

                        curr_page_key = String::from_utf8_lossy(new_key).to_string();
                        self.idx_page_builder.reset();

                        // Add the entry that failed to the new page (must succeed)
                        tracing::debug!("Adding base entry to new index page after split: key_len={}, page_id={}, is_leaf={}",
                                      new_key.len(), new_page_id, is_leaf_index);
                        let success = self.idx_page_builder.add(new_key, new_page_id, is_leaf_index);
                        assert!(success, "Failed to add entry to empty index page");
                    }
                }

                if let Some(ref mut iter) = base_page_iter {
                    is_base_iter_valid = iter.next();
                }
            }
        }

        // Process remaining changes
        while let Some(change) = curr_change {
            if !matches!(change.op, WriteOp::Delete) {
                let new_key = change.key.as_ref();
                let new_page_id = change.page_id;

                // Add the actual key entry (not for leftmost pointer)
                // Check if page is full
                if !self.idx_page_builder.add(new_key, new_page_id, is_leaf_index) {
                    // Page is full, finish current page
                    if let Some(page_data) = prev_page_data.take() {
                        // Flush previous page
                        self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                    }

                    prev_page_key = curr_page_key.clone();
                    prev_page_data = Some(self.idx_page_builder.finish().to_vec());
                    // COW: Always allocate new page for index pages
                    let old_prev = prev_page_id;
                    prev_page_id = self.page_mapper.allocate_page()?;
                    if old_prev != MAX_PAGE_ID {
                        // TODO: Add to GC list instead of immediate free
                        // self.free_page(old_prev);
                    }

                    curr_page_key = String::from_utf8_lossy(new_key).to_string();
                    self.idx_page_builder.reset();

                    // Add leftmost pointer without key
                    self.idx_page_builder.add(b"", new_page_id, is_leaf_index);
                }
            }
            curr_change = change_iter.next();
        }

        // Process remaining change entries (important for empty tree case!)
        while let Some(change) = curr_change {
            if change.op == WriteOp::Upsert {
                let new_key = &change.key;
                let new_page_id = change.page_id;

                // For the first entry in an empty index page, just use it as leftmost pointer
                // We don't need separate key entries when there's only one data page
                if self.idx_page_builder.is_empty() {
                    tracing::debug!("Adding leftmost pointer for page_id={}", new_page_id);
                    self.idx_page_builder.add(b"", new_page_id, is_leaf_index);
                    // For single-page case, we're done - no need to add key entry
                } else {
                    // Add the actual key entry for multi-page case
                    // Check if page is full
                    if !self.idx_page_builder.add(new_key, new_page_id, is_leaf_index) {
                        // Page is full, finish current page
                        if let Some(page_data) = prev_page_data.take() {
                            // Flush previous page
                            self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                        }

                        prev_page_key = curr_page_key.clone();
                        prev_page_data = Some(self.idx_page_builder.finish().to_vec());
                        // COW: Always allocate new page for index pages
                        let old_prev = prev_page_id;
                        prev_page_id = self.page_mapper.allocate_page()?;
                        if old_prev != MAX_PAGE_ID {
                            // TODO: Add to GC list instead of immediate free
                            // self.free_page(old_prev);
                        }

                        curr_page_key = String::from_utf8_lossy(new_key).to_string();
                        self.idx_page_builder.reset();

                        // Add leftmost pointer without key
                        self.idx_page_builder.add(b"", new_page_id, is_leaf_index);
                    }
                }
            }
            curr_change = change_iter.next();
        }

        // Handle final page
        let mut new_root_id = None;
        if self.idx_page_builder.is_empty() {
            // All entries deleted, free the page
            if let Some(page) = stack_entry.idx_page {
                self.free_page(page.get_page_id());
            }

            // Notify parent about deletion
            if !self.stack.is_empty() {
                if let Some(parent) = self.stack.last_mut() {
                    let page_key = parent.idx_page_iter.key().unwrap_or(b"");
                    parent.changes.push(IndexOp {
                        key: Bytes::copy_from_slice(page_key),
                        page_id: prev_page_id,
                        op: WriteOp::Delete,
                    });
                }
            }
        } else {
            // Finish and flush the final page
            let splited = prev_page_data.is_some();

            // Finish current builder content
            if !self.idx_page_builder.is_empty() {
                let final_page = self.idx_page_builder.finish().to_vec();

                if let Some(prev_data) = prev_page_data {
                    // We have a previous page to flush first
                    self.flush_index_page(prev_page_id, prev_data, prev_page_key.as_bytes(), true).await?;

                    // Now handle the final page
                    let final_page_id = self.page_mapper.allocate_page()?;
                    self.flush_index_page(final_page_id, final_page, curr_page_key.as_bytes(), false).await?;
                    new_root_id = Some(final_page_id);
                } else {
                    // This is the only page - COW: Always allocate new page for modified pages
                    let old_page = prev_page_id;
                    prev_page_id = self.page_mapper.allocate_page()?;
                    tracing::debug!("pop_stack: allocated new page_id={} (was {})", prev_page_id, old_page);

                    // Mark old page for freeing (but don't free immediately - GC will handle it)
                    if old_page != MAX_PAGE_ID {
                        // TODO: Add to garbage collection list instead of immediate free
                        // For now, don't free to preserve data
                    }

                    self.flush_index_page(prev_page_id, final_page, curr_page_key.as_bytes(), false).await?;
                    new_root_id = Some(prev_page_id);
                }
            } else if let Some(prev_data) = prev_page_data {
                // COW: Always allocate a new page for modified index pages
                let new_page_id = self.page_mapper.allocate_page()?;
                // Mark for GC instead of immediate free
                if prev_page_id != MAX_PAGE_ID {
                    // TODO: Add to GC list
                    // self.free_page(prev_page_id);
                }
                self.flush_index_page(new_page_id, prev_data, prev_page_key.as_bytes(), false).await?;
                new_root_id = Some(new_page_id);
            }
        }

        Ok(new_root_id)
    }

    /// Seek to leaf page (following C++ Seek)
    async fn seek(&mut self, key: &Key) -> Result<PageId> {
        tracing::debug!("seek: stack.len()={}", self.stack.len());
        // Check if we have an empty tree (no index page)
        if self.stack.is_empty() {
            tracing::debug!("seek: stack is empty, returning error");
            return Err(Error::InvalidState("Stack is empty".into()));
        }

        let stack_entry = self.stack.last_mut().unwrap();
        if stack_entry.idx_page.is_none() {
            // Empty tree case - no index pages yet
            tracing::debug!("seek: idx_page is None, marking as leaf and returning MAX");
            stack_entry.is_leaf_index = true;
            return Ok(PageId::MAX);
        }

        // Navigate down the index tree to find the leaf
        loop {
            let stack_len = self.stack.len();
            let should_continue = {
                let stack_entry = &mut self.stack[stack_len - 1];

                // Seek to the key position in current index page
                if let Some(ref idx_page) = stack_entry.idx_page {
                    // Create a new iterator for this page
                    let mut iter = crate::index::index_page_iter::IndexPageIter::new(idx_page.page_ptr(), self.options.comparator());
                    iter.seek(key.as_ref());
                    let page_id = iter.get_page_id();

                    if page_id == PageId::MAX {
                        // This shouldn't happen in a valid index
                        return Err(Error::InvalidState("Invalid page ID in index".into()));
                    }

                    // Check if this index page points to leaf pages
                    if idx_page.is_pointing_to_leaf() {
                        // We've reached the leaf level
                        stack_entry.is_leaf_index = true;
                        stack_entry.idx_page_iter = unsafe {
                            std::mem::transmute::<IndexPageIter<'_>, IndexPageIter<'static>>(iter)
                        };
                        return Ok(page_id);
                    }

                    // Store the iterator
                    stack_entry.idx_page_iter = unsafe {
                        std::mem::transmute::<IndexPageIter<'_>, IndexPageIter<'static>>(iter)
                    };

                    // Need to load next level
                    Some(page_id)
                } else {
                    None
                }
            };

            if let Some(page_id) = should_continue {
                // Load the next index page
                let mapper = self.cow_meta.as_ref()
                    .and_then(|m| m.mapper.as_ref())
                    .ok_or_else(|| Error::InvalidState("No mapper available".into()))?;

                let next_idx_page = self.index_manager
                    .find_page(&mapper.snapshot(), page_id).await?;

                next_idx_page.pin();

                // Push new stack entry for the next level
                let new_entry = IndexStackEntry::new(Some(Arc::new(*next_idx_page)), self.options.clone());
                self.stack.push(new_entry);
            } else {
                return Err(Error::InvalidState("No index page in stack entry".into()));
            }
        }
    }

    /// Load triple element (following C++ LoadTripleElement)
    async fn load_triple_element(&mut self, idx: usize, page_id: PageId) -> Result<()> {
        if idx >= 3 || self.leaf_triple[idx].is_some() {
            return Ok(());
        }

        assert!(page_id != PageId::MAX);
        let page = self.load_data_page(page_id).await?;
        self.leaf_triple[idx] = Some(page);
        Ok(())
    }

    /// Shift leaf link (following C++ ShiftLeafLink)
    async fn shift_leaf_link(&mut self) -> Result<()> {
        if let Some(page) = self.leaf_triple[0].take() {
            // Write page to disk
            tracing::info!("shift_leaf_link: writing page_id={}", page.page_id());
            self.write_page(page).await?;
        } else {
            tracing::debug!("shift_leaf_link: no page in leaf_triple[0]");
        }
        // Move element 1 to element 0
        self.leaf_triple[0] = self.leaf_triple[1].take();
        // Move element 2 to element 1 (for future use if needed)
        self.leaf_triple[1] = self.leaf_triple[2].take();
        Ok(())
    }

    /// Update leaf link (following C++ LeafLinkUpdate)
    fn leaf_link_update(&mut self, mut page: DataPage) {
        if let Some(ref applying) = self.applying_page {
            page.set_next_page_id(applying.next_page_id());
            page.set_prev_page_id(applying.prev_page_id());
        }
        self.leaf_triple[1] = Some(page);
    }

    /// Insert into leaf link (following C++ LeafLinkInsert)
    async fn leaf_link_insert(&mut self, mut page: DataPage) -> Result<()> {
        assert!(self.leaf_triple[1].is_none());
        tracing::info!("leaf_link_insert: inserting page_id={}", page.page_id());

        // Handle the mutable borrow carefully
        let (next_id, prev_id) = if let Some(prev_page) = &mut self.leaf_triple[0] {
            let nid = prev_page.next_page_id();
            let pid = prev_page.page_id();
            prev_page.set_next_page_id(page.page_id());
            (nid, pid)
        } else {
            (PageId::MAX, PageId::MAX)
        };

        // Now handle the next page if needed
        if next_id != PageId::MAX {
            self.load_triple_element(2, next_id).await?;
            if let Some(next_page) = &mut self.leaf_triple[2] {
                next_page.set_prev_page_id(page.page_id());
            }
        }

        // Set page links
        page.set_prev_page_id(prev_id);
        page.set_next_page_id(next_id);

        self.leaf_triple[1] = Some(page);
        Ok(())
    }

    /// Load data page
    async fn load_data_page(&self, page_id: PageId) -> Result<DataPage> {
        // Get mapping and load from disk
        // Use our page_mapper which should have been restored from COW metadata
        let snapshot = self.page_mapper.snapshot();

        if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
            let page_data = self.file_manager
                .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                .await?;
            let data_page = DataPage::from_page(page_id, page_data);


            Ok(data_page)
        } else {
            // Create new page if not found
            Ok(DataPage::new(page_id, 4096))
        }
    }

    /// Write page to disk
    async fn write_page(&mut self, page: DataPage) -> Result<()> {
        let page_id = page.page_id();

        // Ensure file exists first
        if self.file_manager.get_metadata(0).await.is_err() {
            tracing::debug!("Creating file 0");
            self.file_manager.create_file(&self.table_id).await?;
        }

        // First check if page is already mapped
        let snapshot = self.page_mapper.snapshot();
        let file_page_id = if let Ok(fid) = snapshot.to_file_page(page_id) {
            tracing::debug!("Page {} already mapped to file page {:?}", page_id, fid);
            fid
        } else {
            // Need to allocate a file page for this logical page
            tracing::debug!("Allocating file page for page_id={}", page_id);

            // Switch to file 0 if needed
            self.page_mapper.switch_file(0)?;
            let file_page_id = self.page_mapper.allocate_file_page()?;
            self.page_mapper.map_page(page_id, file_page_id)?;
            self.page_mapper.update_mapping(page_id, file_page_id);
            // Track in manifest builder (following C++ UpdateMapping)
            self.manifest_builder.update_mapping(page_id, file_page_id);
            tracing::debug!("Mapped page {} to file page {:?}", page_id, file_page_id);
            file_page_id
        };

        // Now write the page

        self.file_manager
            .write_page(file_page_id.file_id() as u64, file_page_id.page_offset(), page.as_page())
            .await?;

        tracing::info!("Successfully wrote page {} to disk at file_id={}, offset={}",
            page_id, file_page_id.file_id(), file_page_id.page_offset());
        Ok(())
    }



    /// Apply batch (following C++ Apply)
    async fn apply(&mut self) -> Result<()> {
        tracing::debug!("BatchWriteTask::apply starting with {} entries", self.entries.len());

        // Following C++ Apply() exactly
        // 1. Make COW root
        self.cow_meta = Some(self.index_manager.make_cow_root(&self.table_id)?);

        // CRITICAL: Initialize our page_mapper with existing mappings from COW metadata
        if let Some(ref cow_meta) = self.cow_meta {
            if let Some(ref mapper) = cow_meta.mapper {
                tracing::info!("Initializing page_mapper with existing mappings from COW metadata");
                // Copy all mappings from the COW metadata's mapper to our page_mapper
                let snapshot = mapper.snapshot();
                self.page_mapper.restore_from_snapshot(&snapshot);
            }
        }

        // 2. Sort entries before applying (following C++ line 1660: std::sort(data_batch.begin(), data_batch.end()))
        let comparator = self.index_manager.get_comparator();
        self.entries.sort_by(|a, b| {
            use std::cmp::Ordering;
            match comparator.compare(&a.key, &b.key) {
                x if x < 0 => Ordering::Less,
                x if x > 0 => Ordering::Greater,
                _ => Ordering::Equal,
            }
        });
        tracing::info!("Sorted {} entries before applying", self.entries.len());

        // Debug: Log first few sorted entries
        for (i, entry) in self.entries.iter().take(5).enumerate() {
            tracing::info!("  Sorted entry[{}]: key='{}', op={:?}",
                          i, String::from_utf8_lossy(&entry.key), entry.op);
        }

        // 3. Apply main batch
        let root_id = self.cow_meta.as_ref().unwrap().root_id;
        tracing::warn!("BatchWriteTask::apply - Starting with root_id={}, processing {} entries",
                      root_id, self.entries.len());
        self.apply_batch(root_id, true).await?;

        // 3. Apply TTL batch if any
        self.apply_ttl_batch().await?;

        // 4. Update metadata
        self.update_meta().await?;

        tracing::debug!("BatchWriteTask::apply completed");
        Ok(())
    }

    /// Apply TTL batch
    async fn apply_ttl_batch(&mut self) -> Result<()> {
        if !self.ttl_batch.is_empty() {
            // Sort TTL batch
            self.ttl_batch.sort_by(|a, b| a.key.cmp(&b.key));

            let ttl_root = self.cow_meta.as_ref().unwrap().ttl_root_id;
            let saved_entries = self.entries.clone();
            self.entries = self.ttl_batch.clone();
            self.apply_batch(ttl_root, false).await?;
            self.entries = saved_entries;
            self.ttl_batch.clear();
        }
        Ok(())
    }

    /// Apply batch to tree (following C++ ApplyBatch)
    async fn apply_batch(&mut self, root_id: PageId, update_ttl: bool) -> Result<()> {
        tracing::info!("apply_batch: root_id={}, update_ttl={}, entries={}",
                        root_id, update_ttl, self.entries.len());

        // Initialize stack with root
        if root_id != PageId::MAX {
            // Load root index page
            // Following C++ implementation to load from index manager
            tracing::debug!("Loading existing root index page with root_id={}", root_id);

            // Load the existing root index page from disk
            // CRITICAL: Use the snapshot from COW metadata, not our empty page_mapper!
            let snapshot = if let Some(ref cow_meta) = self.cow_meta {
                if let Some(ref mapper) = cow_meta.mapper {
                    tracing::info!("Using mapper from COW metadata to find root page");
                    mapper.snapshot()
                } else {
                    tracing::warn!("No mapper in COW metadata, using empty snapshot");
                    self.page_mapper.snapshot()
                }
            } else {
                tracing::warn!("No COW metadata available, using empty snapshot");
                self.page_mapper.snapshot()
            };

            tracing::debug!("About to call find_page for root_id={}", root_id);
            let idx_page = match self.index_manager.find_page(&snapshot, root_id).await {
                Ok(page) => {
                    tracing::debug!("Successfully loaded root index page");
                    page
                },
                Err(e) => {
                    tracing::error!("Failed to load root index page: {:?}", e);
                    return Err(e);
                }
            };

            // Create stack entry with the loaded index page (convert Box to Arc)
            let mut entry = IndexStackEntry::new(Some(Arc::from(idx_page)), self.options.clone());

            // Check if this index points to leaf pages (data pages) or other index pages
            if let Some(ref page) = entry.idx_page {
                entry.is_leaf_index = page.is_pointing_to_leaf();
                tracing::debug!("Loaded root index page, is_leaf_index={}", entry.is_leaf_index);
            }

            self.stack.push(entry);
        } else {
            // Empty tree
            tracing::debug!("Creating new empty tree");
            let mut entry = IndexStackEntry::new(None, self.options.clone());
            entry.is_leaf_index = true;
            self.stack.push(entry);
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut cidx = 0;
        while cidx < self.entries.len() {
            //     cidx + 1, self.entries.len(),
            //     String::from_utf8_lossy(&self.entries[cidx].key));
            tracing::info!("Processing entry {} of {}: key={:?}",
                cidx + 1, self.entries.len(),
                String::from_utf8_lossy(&self.entries[cidx].key));
            let batch_start_key = self.entries[cidx].key.clone();

            if self.stack.len() > 1 {
                tracing::debug!("Seeking stack for key");
                self.seek_stack(&batch_start_key).await?;
            }

            tracing::debug!("Calling seek for key");
            let page_id = self.seek(&batch_start_key).await?;
            tracing::debug!("seek returned page_id={}", page_id);

            if page_id != PageId::MAX {
                tracing::debug!("Loading applying page {}", page_id);
                self.load_applying_page(page_id).await?;
            }

            tracing::debug!("About to call apply_one_page at cidx={}", cidx);
            let cidx_before = cidx;
            self.apply_one_page(&mut cidx, now_ms, update_ttl).await?;
            tracing::info!("apply_one_page completed, cidx now={}", cidx);

            // Safety check: cidx must advance
            if cidx == cidx_before {
                break;
            }
        }

        // Flush remaining pages
        self.shift_leaf_link().await?;
        self.shift_leaf_link().await?;

        // Update root by popping all stack entries
        let mut final_root_id = None;
        while !self.stack.is_empty() {
            if let Some(root_id) = self.pop_stack().await? {
                tracing::debug!("pop_stack returned new root_id={}", root_id);
                final_root_id = Some(root_id);
            }
        }

        // Update COW metadata with new root if changed
        if let Some(new_root) = final_root_id {
            tracing::warn!("Updating COW metadata with new root={}", new_root);
            if let Some(ref mut meta) = self.cow_meta {
                tracing::warn!("BatchWriteTask: Setting root_id from {} to {}", meta.root_id, new_root);
                meta.root_id = new_root;
            }
        }

        Ok(())
    }

    /// Load applying page (following C++ LoadApplyingPage)
    async fn load_applying_page(&mut self, page_id: PageId) -> Result<()> {
        assert!(page_id != PageId::MAX);

        // Check if page is already in leaf_triple[1]
        if let Some(ref page) = self.leaf_triple[1] {
            if page.page_id() == page_id {
                // Fast path: move it to applying_page
                self.applying_page = self.leaf_triple[1].take();
                return Ok(());
            }
        }

        // Load from disk
        let page = self.load_data_page(page_id).await?;


        self.applying_page = Some(page);

        // Shift link if needed
        if self.leaf_triple[1].is_some() {
            self.shift_leaf_link().await?;
        }

        Ok(())
    }

    /// Apply one page following C++ ApplyOnePage pattern exactly
    /// This processes entries streaming-style and handles page splits inline
    async fn apply_one_page(&mut self, cidx: &mut usize, now_ms: u64, update_ttl: bool) -> Result<()> {
        tracing::info!("apply_one_page: starting at cidx={}, total entries={}", *cidx, self.entries.len());

        // Reset the page builder
        self.data_page_builder.reset();

        // Get comparator
        let comparator = self.index_manager.get_comparator();

        // Initialize base page iterator if we have an applying page
        // We need to work around the borrow checker here
        let base_page_id = self.applying_page.as_ref().map(|p| p.page_id()).unwrap_or(PageId::MAX);

        // We need to handle the applying page carefully to avoid lifetime issues
        // The issue is that DataPageIterator borrows the page, so we can't move it
        // Solution: Process the page entries into a vector first
        let mut base_entries = Vec::new();
        if let Some(ref page) = self.applying_page {
            let mut temp_iter = DataPageIterator::new(page);
            while let Some((k, v, ts, exp)) = temp_iter.next() {
                tracing::info!("Loading base entry: key='{}'", String::from_utf8_lossy(&k));
                base_entries.push((k, v, ts, exp));
            }
            tracing::info!("Loaded {} entries from base page", base_entries.len());
        }
        let mut base_idx = 0;
        let has_base = !base_entries.is_empty();

        // Get the page key for index entry
        let page_key = if let Some(entry) = self.stack.last() {
            Bytes::copy_from_slice(entry.idx_page_iter.key().unwrap_or(&[]))
        } else {
            Bytes::new()
        };
        let mut curr_page_key = page_key.clone();
        let mut prev_key = Bytes::new();

        // Initialize page_id - for COW, we always allocate new pages when modifying
        // We only track the old page_id to know what to free later
        let old_page_id = if has_base {
            Some(base_page_id)
        } else {
            None
        };
        // Always start with MAX to force new allocation
        let mut page_id = PageId::MAX;

        // Process entries one by one, following C++ lines 476+
        let change_end = self.entries.len();
        let mut entries_added = 0;

        tracing::info!("Starting merge: base_entries={}, change_entries={} (from cidx={})",
                     base_entries.len(), change_end - *cidx, *cidx);

        while base_idx < base_entries.len() || *cidx < change_end {
            let mut should_add = false;
            let mut key_to_add = Bytes::new();
            let mut val_to_add = Bytes::new();
            let mut ts_to_add = 0u64;
            let mut expire_to_add = 0u64;
            let mut is_overflow = false;
            let mut advance_base = false;
            let mut advance_change = false;

            // Merge logic - compare base and change entries
            if base_idx < base_entries.len() && *cidx < change_end {
                let (ref base_key, _, _, _) = base_entries[base_idx];
                let change_entry = &self.entries[*cidx];
                let cmp = comparator.compare(base_key, &change_entry.key);

                if cmp < 0 {
                    // Base key comes first
                    let (ref k, ref v, ts, exp) = base_entries[base_idx];
                    key_to_add = k.clone();
                    val_to_add = v.clone();
                    ts_to_add = ts;
                    expire_to_add = exp.unwrap_or(0);
                    is_overflow = false; // TODO: track overflow flag properly
                    should_add = true;
                    advance_base = true;
                } else if cmp == 0 {
                    // Same key - apply change
                    advance_base = true;
                    advance_change = true;

                    // Get base entry values
                    let (_, ref base_val, base_ts, base_exp) = base_entries[base_idx];
                    // Set key from base since we're deleting/updating existing key
                    key_to_add = base_key.clone();

                    match change_entry.op {
                        WriteOp::Delete => {
                            // Delete - don't add
                            should_add = false;
                        }
                        WriteOp::Upsert => {
                            // Update with new value
                            // key_to_add already set from base
                            val_to_add = change_entry.value.clone();
                            ts_to_add = change_entry.timestamp;
                            expire_to_add = change_entry.expire_ts;
                            should_add = true;
                        }
                    }
                } else {
                    // Change key comes first
                    advance_change = true;

                    match change_entry.op {
                        WriteOp::Delete => {
                            // Deleting non-existent key - skip
                            should_add = false;
                        }
                        WriteOp::Upsert => {
                            key_to_add = change_entry.key.clone();
                            val_to_add = change_entry.value.clone();
                            ts_to_add = change_entry.timestamp;
                            expire_to_add = change_entry.expire_ts;
                            should_add = true;
                        }
                    }
                }
            } else if base_idx < base_entries.len() {
                // Only base entries left
                let (ref k, ref v, ts, exp) = base_entries[base_idx];
                key_to_add = k.clone();
                val_to_add = v.clone();
                ts_to_add = ts;
                expire_to_add = exp.unwrap_or(0);
                is_overflow = false; // TODO: track overflow flag properly
                should_add = true;
                advance_base = true;
            } else if *cidx < change_end {
                // Only change entries left
                let change_entry = &self.entries[*cidx];
                advance_change = true;

                match change_entry.op {
                    WriteOp::Delete => {
                        // Deleting non-existent key - skip
                        should_add = false;
                    }
                    WriteOp::Upsert => {
                        key_to_add = change_entry.key.clone();
                        val_to_add = change_entry.value.clone();
                        ts_to_add = change_entry.timestamp;
                        expire_to_add = change_entry.expire_ts;
                        should_add = true;
                    }
                }
            }

            // Add the entry if needed - following C++ add_to_page lambda
            if should_add {
                // Check for expired entries
                if expire_to_add != 0 && expire_to_add <= now_ms {
                    // Skip expired entries
                    if update_ttl {
                        self.update_ttl(expire_to_add, &key_to_add, WriteOp::Delete);
                    }
                } else {
                    // Handle overflow if needed
                    let (val_for_page, is_overflow_entry) = if !is_overflow && self.is_overflow_kv(&key_to_add, &val_to_add) {
                        // Write overflow value
                        self.write_overflow_value(&val_to_add).await?;
                        (Bytes::copy_from_slice(&self.overflow_ptrs), true)
                    } else {
                        (val_to_add.clone(), is_overflow)
                    };

                    // Try to add to the current page
                    entries_added += 1;
                    tracing::info!("Adding entry #{} to data page: key='{}', val_len={}, ts={}",
                                  entries_added, String::from_utf8_lossy(&key_to_add), val_for_page.len(), ts_to_add);
                    let added = self.data_page_builder.add(
                        &key_to_add,
                        &val_for_page,
                        ts_to_add,
                        if expire_to_add > 0 { Some(expire_to_add) } else { None },
                        is_overflow_entry,
                    );

                    if !added {
                        // Page is full - following C++ lines 458-469
                        // Finish current page
                        self.finish_data_page(curr_page_key, page_id).await?;

                        // Start new page
                        // Use FindShortestSeparator like C++ does
                        let mut separator = prev_key.to_vec();
                        comparator.find_shortest_separator(&mut separator, &key_to_add);
                        curr_page_key = Bytes::from(separator);
                        self.data_page_builder.reset();

                        // Add to new page (must succeed)
                        let success = self.data_page_builder.add(
                            &key_to_add,
                            &val_for_page,
                            ts_to_add,
                            if expire_to_add > 0 { Some(expire_to_add) } else { None },
                            is_overflow_entry,
                        );
                        assert!(success, "Failed to add entry to empty page");

                        // Set page_id to MAX for new page allocation
                        page_id = PageId::MAX;
                    }

                    prev_key = key_to_add.clone();

                    // Update TTL if needed
                    if update_ttl && expire_to_add > 0 {
                        self.update_ttl(expire_to_add, &key_to_add, WriteOp::Upsert);
                    }
                }
            }

            // Advance iterators
            if advance_base {
                base_idx += 1;
            }
            if advance_change {
                *cidx += 1;
            }

            // Safety check
            if !advance_base && !advance_change {
                return Err(Error::InvalidState("No progress in merge loop".into()));
            }
        }

        // Finish the last page if there's content
        if !self.data_page_builder.is_empty() {
            tracing::info!("Finishing final data page with {} entries, key='{}', page_id={}",
                         entries_added, String::from_utf8_lossy(&curr_page_key), page_id);
            self.finish_data_page(curr_page_key, page_id).await?;
        } else {
            tracing::info!("No entries to finish (entries_added={})", entries_added);
        }

        // Mark old page for GC if we modified an existing page
        if let Some(old_id) = old_page_id {
            // TODO: Add to GC list instead of immediate free
        }

        // Don't clear applying_page here - it's managed by the caller
        // self.applying_page = None;

        Ok(())
    }

    /// Update TTL (helper for ApplyOnePage)
    fn update_ttl(&mut self, expire_ts: u64, key: &[u8], op: WriteOp) {
        if expire_ts > 0 {
            self.ttl_batch.push(WriteDataEntry {
                key: Bytes::copy_from_slice(key),
                value: if matches!(op, WriteOp::Upsert) { Bytes::copy_from_slice(key) } else { Bytes::new() },
                op,
                timestamp: 0,
                expire_ts,
            });
        }
    }

    /// Check if a key-value pair would overflow (following C++ IsOverflowKV)
    fn is_overflow_kv(&self, key: &[u8], value: &[u8]) -> bool {
        use crate::page::HEADER_SIZE;

        // Minimum reserved space for header and restart array
        let reserved = HEADER_SIZE + (2 * 2); // header + at least 1 restart entry

        // Check if value is too large for a single page
        if reserved + value.len() > self.options.data_page_size {
            return true;
        }

        // Also consider the key size and encoding overhead
        let total_size = reserved + key.len() + value.len() + 16; // 16 bytes for varint encoding overhead
        total_size > self.options.data_page_size
    }

    /// Write overflow value to overflow pages (following C++ WriteOverflowValue)
    async fn write_overflow_value(&mut self, value: &[u8]) -> Result<()> {
        use crate::page::{OverflowPage, HEADER_SIZE};

        tracing::debug!("write_overflow_value called with value_size={}", value.len());

        // Clear overflow pointers buffer
        self.overflow_ptrs.clear();

        // Calculate page capacity
        let page_cap = self.options.data_page_size - HEADER_SIZE - 100; // Reserve space for metadata
        tracing::debug!("page_cap={}, data_page_size={}", page_cap, self.options.data_page_size);

        let mut remaining = value;
        let mut head_pointers = Vec::new();

        while !remaining.is_empty() {
            // Calculate how many bytes we can fit in this page
            let chunk_size = remaining.len().min(page_cap);
            tracing::debug!("Writing chunk, remaining={}, chunk_size={}", remaining.len(), chunk_size);

            // Allocate page for overflow
            let page_id = self.page_mapper.allocate_page()?;
            head_pointers.push(page_id);
            tracing::debug!("Allocated overflow page_id={}", page_id);

            // Create overflow page with chunk of value
            let mut overflow_page = OverflowPage::new(page_id, self.options.data_page_size);
            overflow_page.set_value_data(&remaining[..chunk_size]);

            // Write page to disk
            let file_page_id = self.page_mapper.allocate_file_page()?;
            self.page_mapper.map_page(page_id, file_page_id)?;
            self.page_mapper.update_mapping(page_id, file_page_id);
            self.manifest_builder.update_mapping(page_id, file_page_id);

            let page = overflow_page.to_page();
            self.file_manager.write_page(
                file_page_id.file_id() as u64,
                file_page_id.page_offset(),
                &page
            ).await?;

            remaining = &remaining[chunk_size..];
        }

        // Encode pointers into overflow_ptrs buffer (following C++ PutFixed32)
        for page_id in head_pointers {
            self.overflow_ptrs.extend_from_slice(&page_id.to_le_bytes());
        }

        Ok(())
    }

    /// Update metadata (following C++ UpdateMeta)
    async fn update_meta(&mut self) -> Result<()> {
        // First flush manifest (following C++ FlushManifest)
        self.flush_manifest().await?;

        // Trigger compaction if needed (following C++ CompactIfNeeded)
        self.compact_if_needed().await;

        // Update the COW metadata in index manager
        if let Some(mut cow_meta) = self.cow_meta.take() {
            // CRITICAL: Clone the page_mapper to preserve the mappings!
            // This is what allows the next write to find the pages
            cow_meta.mapper = Some(Box::new((*self.page_mapper).clone()));

            tracing::info!("update_meta: Updating root for table {:?} with root_id={}, has mapper={}",
                self.table_id, cow_meta.root_id, cow_meta.mapper.is_some());

            // The mapper should contain the page mappings!
            if let Some(ref mapper) = cow_meta.mapper {
                tracing::info!("update_meta: Mapper now contains page mappings");
            }

            self.index_manager.update_root(&self.table_id, cow_meta);
        } else {
            tracing::warn!("update_meta: No COW metadata to update for table {:?}", self.table_id);
        }
        Ok(())
    }

    /// Check and trigger compaction if needed (following C++ CompactIfNeeded)
    async fn compact_if_needed(&self) {
        // Check if compaction is enabled (use default factor of 2.0)
        let file_amplify_factor = 2.0;
        if !self.options.data_append_mode || file_amplify_factor == 0.0 {
            return;
        }

        // Get current file usage stats
        let mapping_count = self.page_mapper.mapping_count();
        let allocated_pages = self.page_mapper.allocated_pages();

        // Calculate amplification factor
        if allocated_pages > 0 {
            let amplify_factor = mapping_count as f64 / allocated_pages as f64;

            // Trigger compaction if amplification exceeds threshold
            if amplify_factor > file_amplify_factor {
                tracing::info!("Triggering compaction for table {:?}, amplify_factor={:.2}",
                    self.table_id, amplify_factor);

                // Schedule compaction task
                // In C++, this calls shard->AddPendingCompact(table_id) which:
                // 1. Creates a CompactRequest for this table
                // 2. Adds it to the pending_queues_ for sequential processing
                // 3. The shard processes it as RequestType::Compact
                // 4. This triggers BackgroundWrite::CompactDataFile()
                //
                // For now, we trigger file GC directly which performs similar cleanup
                // but doesn't do the full data compaction that moves pages to new files.
                self.trigger_file_gc().await;
            }
        }
    }

    /// Trigger file garbage collection (following C++ TriggerFileGC)
    async fn trigger_file_gc(&self) {
        tracing::info!("Triggering file GC for table {:?}", self.table_id);

        // Get retained files from current mappings
        let mut retained_files = std::collections::HashSet::new();

        // Walk through all mappings to find which files are still in use
        let mapping = self.page_mapper.snapshot();
        for page_id in 0..mapping.max_page_id() {
            if let Ok(file_page_id) = mapping.to_file_page(page_id) {
                let file_id = file_page_id.file_id() as u64;  // Convert u32 to u64
                retained_files.insert(file_id);
            }
        }

        // Get current file ID
        let current_file_id = self.page_mapper.current_file_id();

        // Submit task to file GC worker if available (following C++ write_task.cpp:352)
        if let Some(ref file_gc) = self.file_gc {
            // Get timestamp for GC
            let ts = chrono::Utc::now().timestamp_micros() as u64;

            // Submit task to GC
            let gc = file_gc.lock().await;
            if gc.add_task(self.table_id.clone(), ts, current_file_id, retained_files.clone()) {
                tracing::info!("Submitted GC task for table {:?} with {} retained files",
                    self.table_id, retained_files.len());
            } else {
                tracing::warn!("Failed to submit GC task for table {:?}", self.table_id);
            }
        } else {
            tracing::debug!("File GC not enabled, skipping GC task submission");
        }
    }

    /// Flush manifest to disk (following C++ FlushManifest)
    async fn flush_manifest(&mut self) -> Result<()> {
        // Check if manifest builder is empty (following C++ line 211-214)
        if self.manifest_builder.is_empty() {
            return Ok(());
        }

        let manifest_size = self.cow_meta.as_ref()
            .map(|m| m.manifest_size)
            .unwrap_or(0);

        // Get current root IDs
        let root_id = self.cow_meta.as_ref()
            .map(|m| m.root_id)
            .unwrap_or(MAX_PAGE_ID);
        let ttl_root_id = self.cow_meta.as_ref()
            .map(|m| m.ttl_root_id)
            .unwrap_or(MAX_PAGE_ID);

        // Following C++ logic: append if under limit, otherwise switch
        // Use a reasonable default manifest limit (64KB like C++)
        let manifest_limit = 64 * 1024;

        if manifest_size > 0 &&
           manifest_size + self.manifest_builder.current_size() as u64 <= manifest_limit {
            // Append to existing manifest (following C++ line 223-227)
            let blob = self.manifest_builder.finalize(root_id, ttl_root_id);
            self.file_manager.append_manifest(
                &self.table_id,
                &blob,
                manifest_size
            ).await?;

            // Update manifest size
            if let Some(ref mut meta) = self.cow_meta {
                meta.manifest_size += blob.len() as u64;
            }
        } else {
            // Switch to new manifest (following C++ line 231-238)
            let snapshot = self.page_mapper.snapshot();
            let max_fp_id = self.page_mapper.allocate_file_page()?;

            let snapshot_data = self.manifest_builder.snapshot(
                root_id,
                ttl_root_id,
                &snapshot,
                max_fp_id
            );

            self.file_manager.switch_manifest(
                &self.table_id,
                &snapshot_data
            ).await?;

            // Update manifest size
            if let Some(ref mut meta) = self.cow_meta {
                meta.manifest_size = snapshot_data.len() as u64;
            }
        }

        // Reset manifest builder for next batch
        self.manifest_builder.reset();

        Ok(())
    }

    /// Finish current data page and create index entry
    async fn finish_data_page(&mut self, page_key: Bytes, mut page_id: PageId) -> Result<()> {
        // Following C++ FinishDataPage exactly
        let cur_page_len = self.data_page_builder.current_size_estimate();
        let builder = std::mem::replace(&mut self.data_page_builder, DataPageBuilder::new(4096));

        // In COW, we always allocate a new page when modifying
        // If we had an existing page, we need to free it
        let old_page_id = if page_id != PageId::MAX {
            Some(page_id)
        } else {
            None
        };

        // Always allocate a new page for COW
        page_id = self.page_mapper.allocate_page()?;
        let was_new_allocation = old_page_id.is_none();

        let data_page = builder.finish(page_id);
        tracing::info!("Built data page id={} with content_length={} bytes, restart_count={}",
            page_id, data_page.content_length(), data_page.restart_count());

        // Debug: List entries in this page
        let mut iter = DataPageIterator::new(&data_page);
        let mut count = 0;
        while let Some((k, _, _, _)) = iter.next() {
            if count < 3 {
                tracing::info!("  Page {} entry[{}]: key='{}'", page_id, count, String::from_utf8_lossy(&k));
            }
            count += 1;
        }
        tracing::info!("  Page {} total entries: {}", page_id, count);

        // Check if we should redistribute with previous page
        let one_quarter = self.options.data_page_size >> 2;
        let three_quarter = self.options.data_page_size - one_quarter;

        if cur_page_len < one_quarter as usize {
            if let Some(prev_page) = &self.leaf_triple[0] {
                if prev_page.restart_num() > 1 && prev_page.content_length() as usize > three_quarter as usize {
                    // Redistribute pages - for now just proceed
                    // Full implementation would redistribute entries
                }
            }
        }

        // Update index with this page
        if !self.stack.is_empty() {
            // Get the first key from the data page to use as index key
            let index_key = if count > 0 {
                // Re-iterate to get first key
                let mut temp_iter = DataPageIterator::new(&data_page);
                if let Some((first_key, _, _, _)) = temp_iter.next() {
                    Bytes::copy_from_slice(&first_key)
                } else {
                    page_key.clone()
                }
            } else {
                page_key.clone()
            };

            tracing::debug!("Adding index change: key={:?}, page_id={}",
                String::from_utf8_lossy(&index_key), page_id);
            self.stack.last_mut().unwrap().changes.push(IndexOp {
                key: index_key,
                page_id,
                op: WriteOp::Upsert,
            });
        }

        // Mark old page for GC if we had one (COW semantics)
        if let Some(old_id) = old_page_id {
            // TODO: Add to GC list instead of immediate free
            // self.free_page(old_id);
            tracing::info!("Would free old page_id={}, new page_id={} (deferred for GC)", old_id, page_id);
        }

        // Insert or update in leaf triple (following C++ FinishDataPage logic)
        if was_new_allocation {
            // This is a new data page that does not exist in the tree
            self.leaf_link_insert(data_page).await?;
        } else {
            // This is an existing data page with updated content (COW copy)
            self.leaf_link_update(data_page);
        }

        // Shift leaf link after insertion/update (following C++ which calls ShiftLeafLink)
        self.shift_leaf_link().await?;

        // Debug: Log current leaf_triple state
        tracing::info!("After shift_leaf_link, leaf_triple state:");
        for i in 0..3 {
            if let Some(ref p) = self.leaf_triple[i] {
                tracing::info!("  leaf_triple[{}] = page_id={}", i, p.page_id());
            } else {
                tracing::info!("  leaf_triple[{}] = None", i);
            }
        }

        Ok(())
    }

    /// Finish index page
    async fn finish_index_page(&mut self, prev_page: &mut Option<(PageId, Vec<u8>)>, page_key: Bytes) -> Result<()> {
        // Following C++ FinishIndexPage
        let cur_page_len = self.idx_page_builder.current_size_estimate();
        let page_data = self.idx_page_builder.finish().to_vec();

        // Check if should redistribute
        let one_quarter = self.options.index_page_size >> 2;
        let three_quarter = self.options.index_page_size - one_quarter;

        if cur_page_len < one_quarter as usize {
            if let Some((prev_id, prev_data)) = prev_page {
                if prev_data.len() > three_quarter as usize {
                    // Would redistribute here - for now just proceed
                }
            }
        }

        // Flush previous page if exists
        if let Some((prev_id, prev_data)) = prev_page.take() {
            self.flush_index_page(prev_id, prev_data, &page_key, true).await?;
        }

        // Allocate page for current
        let page_id = self.page_mapper.allocate_page()?;
        *prev_page = Some((page_id, page_data));

        Ok(())
    }

    /// Flush index page to disk
    async fn flush_index_page(&mut self, page_id: PageId, page_data: Vec<u8>, page_key: &[u8], split: bool) -> Result<()> {
        tracing::debug!("flush_index_page: page_id={}, data_len={}, split={}", page_id, page_data.len(), split);
        // Following C++ FlushIndexPage
        // Write the index page to disk
        let mut page = Page::new(self.options.index_page_size);
        let copy_len = page_data.len().min(page.size());
        page.as_bytes_mut()[..copy_len].copy_from_slice(&page_data[..copy_len]);

        // Map and write page
        if self.file_manager.get_metadata(0).await.is_err() {
            tracing::debug!("flush_index_page: creating file for table");
            self.file_manager.create_file(&self.table_id).await?;
        }

        let snapshot = self.page_mapper.snapshot();
        let file_page_id = if let Ok(fid) = snapshot.to_file_page(page_id) {
            tracing::debug!("flush_index_page: page {} already mapped to file_page {}", page_id, fid);
            fid
        } else {
            self.page_mapper.switch_file(0)?;
            let fid = self.page_mapper.allocate_file_page()?;
            self.page_mapper.map_page(page_id, fid)?;
            self.page_mapper.update_mapping(page_id, fid);
            // Track in manifest builder (following C++ UpdateMapping)
            self.manifest_builder.update_mapping(page_id, fid);
            tracing::debug!("flush_index_page: mapped page {} to file_page {}", page_id, fid);
            fid
        };

        tracing::debug!("flush_index_page: writing page {} to disk at file_page {}", page_id, file_page_id);
        self.file_manager.write_page(file_page_id.file_id() as u64, file_page_id.page_offset(), &page).await?;
        tracing::debug!("flush_index_page: successfully wrote page {} to disk", page_id);

        // Update parent if split and (new page or root)
        if split && (page_id == PageId::MAX || self.stack.len() == 1) {
            if self.stack.len() == 1 {
                // Create new root level
                self.stack.insert(0, IndexStackEntry::new(None, self.options.clone()));
            }

            if self.stack.len() >= 2 {
                let parent_index = self.stack.len() - 2;
                self.stack[parent_index].changes.push(IndexOp {
                    key: Bytes::copy_from_slice(page_key),
                    page_id,
                    op: WriteOp::Upsert,
                });
            }
        }

        Ok(())
    }

    /// Free a page
    fn free_page(&mut self, page_id: PageId) {
        // Mark page as free in page mapper
        // Full implementation would handle page recycling
    }
}

#[async_trait]
impl Drop for BatchWriteTask {
    fn drop(&mut self) {
        // Ensure all pages are unpinned when the task is dropped
        self.abort();
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
        tracing::debug!("BatchWriteTask execute: {} entries", self.entries.len());

        // Clone self to get mutable version for processing
        let mut task = self.clone();
        task.apply().await?;

        tracing::debug!("BatchWriteTask completed apply");
        Ok(TaskResult::BatchWrite(self.entries.len()))
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
            let entry = WriteDataEntry {
                key: other_single.key.clone(),
                value: other_single.value.clone(),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            };
            self.entries.push(entry);
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

// Custom Debug implementation for BatchWriteTask
impl std::fmt::Debug for BatchWriteTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchWriteTask")
            .field("entries_count", &self.entries.len())
            .field("table_id", &self.table_id)
            .field("has_cow_meta", &self.cow_meta.is_some())
            .field("stack_depth", &self.stack.len())
            .finish()
    }
}

// Manual Clone implementation for BatchWriteTask
impl Clone for BatchWriteTask {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            table_id: self.table_id.clone(),
            page_cache: self.page_cache.clone(),
            page_mapper: self.page_mapper.clone(),
            file_manager: self.file_manager.clone(),
            index_manager: self.index_manager.clone(),
            stack: Vec::new(),
            leaf_triple: [None, None, None],
            applying_page: None,
            cow_meta: None,
            ttl_batch: Vec::new(),
            data_page_builder: DataPageBuilder::new(4096),
            idx_page_builder: IndexPageBuilder::new(Arc::new(KvOptions::default())),
            options: Arc::new(KvOptions::default()),
            manifest_builder: ManifestBuilder::new(),
            overflow_ptrs: Vec::new(),
            file_gc: self.file_gc.clone(),
        }
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

    async fn execute(&self, ctx: &TaskContext) -> Result<TaskResult> {
        // Implement delete as a batch write with a single delete entry
        let entry = WriteDataEntry {
            key: self.key.clone(),
            value: Bytes::new(), // Empty value for delete
            op: WriteOp::Delete,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            expire_ts: 0,
        };

        // Create a BatchWriteTask with single delete entry
        let batch_task = BatchWriteTask::new(
            vec![entry],
            self.table_id.clone(),
            self.page_cache.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
            // Need index manager - get it from context or create one
            Arc::new(IndexPageManager::new(
                self.file_manager.clone(),
                ctx.options.clone(),
            )),
            None,  // file_gc not available in delete task
        );

        // Execute the batch write
        match batch_task.execute(ctx).await? {
            TaskResult::BatchWrite(_) => Ok(TaskResult::Delete(true)),
            _ => Ok(TaskResult::Delete(false)),
        }
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
    use crate::page::PageCacheConfig;
    use crate::config::KvOptions;

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
        let page_mapper = Arc::new(PageMapper::new());
        let options = Arc::new(KvOptions::default());
        let index_manager = Arc::new(IndexPageManager::new(file_manager.clone(), options.clone()));

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
            index_manager.clone(),
        );

        // Execute
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::BatchWrite(count) => assert_eq!(count, 1),
            _ => panic!("Unexpected result"),
        }
    }

    #[tokio::test]
    async fn test_batch_write_task() {
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
        let options = Arc::new(KvOptions::default());
        let index_manager = Arc::new(IndexPageManager::new(file_manager.clone(), options.clone()));

        // Initialize
        file_manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);

        // Create batch write task
        let entries = vec![
            WriteDataEntry {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            },
            WriteDataEntry {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            },
        ];

        let task = BatchWriteTask::new(
            entries,
            table_id,
            page_cache.clone(),
            page_mapper.clone(),
            file_manager.clone(),
            index_manager.clone(),
            None,  // file_gc not available in test
        );

        // Execute
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::BatchWrite(count) => assert_eq!(count, 2),
            _ => panic!("Unexpected result"),
        }
    }
}