//! Page mapper for logical to physical page ID translation

use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, RwLock};
use crate::types::{PageId, FilePageId, FileId, MAX_PAGE_ID, MAX_FILE_PAGE_ID};
use crate::Result;
use crate::error::Error;

/// Mapping from logical page ID to physical file page ID
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageMapping {
    /// Logical page ID
    pub page_id: PageId,
    /// Physical file page ID
    pub file_page_id: FilePageId,
    /// File ID containing the page
    pub file_id: FileId,
}

/// Snapshot of page mappings at a point in time
#[derive(Clone, Debug)]
pub struct MappingSnapshot {
    /// Page mappings indexed by logical page ID
    mappings: Arc<BTreeMap<PageId, PageMapping>>,
    /// Current maximum page ID
    max_page_id: PageId,
    /// Version number of this snapshot
    version: u64,
}

impl MappingSnapshot {
    /// Create a new empty snapshot
    pub fn new(version: u64) -> Self {
        Self {
            mappings: Arc::new(BTreeMap::new()),
            max_page_id: 0,
            version,
        }
    }

    /// Create from existing mappings
    pub fn from_mappings(mappings: BTreeMap<PageId, PageMapping>, version: u64) -> Self {
        let max_page_id = mappings.keys().max().copied().unwrap_or(0);
        Self {
            mappings: Arc::new(mappings),
            max_page_id,
            version,
        }
    }

    /// Look up physical page ID for a logical page
    pub fn get(&self, page_id: PageId) -> Option<FilePageId> {
        self.mappings.get(&page_id).map(|m| m.file_page_id)
    }

    /// Get the full mapping for a logical page
    pub fn get_mapping(&self, page_id: PageId) -> Option<&PageMapping> {
        self.mappings.get(&page_id)
    }

    /// Get all mappings
    pub fn all_mappings(&self) -> &BTreeMap<PageId, PageMapping> {
        &self.mappings
    }

    /// Get the maximum page ID
    pub fn max_page_id(&self) -> PageId {
        self.max_page_id
    }

    /// Get version number
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Check if a page exists
    pub fn contains(&self, page_id: PageId) -> bool {
        self.mappings.contains_key(&page_id)
    }

    /// Convert logical page ID to file page ID (following C++ ToFilePage)
    pub fn to_file_page(&self, page_id: PageId) -> crate::Result<FilePageId> {
        self.mappings
            .get(&page_id)
            .map(|m| m.file_page_id)
            .ok_or_else(|| crate::error::Error::NotFound)
    }

    /// Get total number of pages
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }
}

/// Page mapper manages logical to physical page mappings
#[derive(Debug, Clone)]
pub struct PageMapper {
    /// Current mappings
    mappings: Arc<RwLock<BTreeMap<PageId, PageMapping>>>,
    /// Next available logical page ID
    next_page_id: Arc<RwLock<PageId>>,
    /// Next available file page ID (as raw u64)
    next_file_page_id: Arc<RwLock<u64>>,
    /// Current file ID
    current_file_id: Arc<RwLock<FileId>>,
    /// Version counter for snapshots
    version_counter: Arc<RwLock<u64>>,
    /// File page allocations by file ID
    file_allocations: Arc<RwLock<HashMap<FileId, Vec<FilePageId>>>>,
}

impl PageMapper {
    /// Create a new page mapper
    pub fn new() -> Self {
        Self {
            mappings: Arc::new(RwLock::new(BTreeMap::new())),
            next_page_id: Arc::new(RwLock::new(0)),
            next_file_page_id: Arc::new(RwLock::new(0)),
            current_file_id: Arc::new(RwLock::new(0)),
            version_counter: Arc::new(RwLock::new(0)),
            file_allocations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Allocate a new logical page
    pub fn allocate_page(&self) -> Result<PageId> {
        let mut next_id = self.next_page_id.write().unwrap();

        if *next_id == MAX_PAGE_ID {
            return Err(Error::StorageFull);
        }

        let page_id = *next_id;
        *next_id += 1;
        Ok(page_id)
    }

    /// Allocate a new file page
    pub fn allocate_file_page(&self) -> Result<FilePageId> {
        let mut next_id = self.next_file_page_id.write().unwrap();

        if *next_id >= MAX_FILE_PAGE_ID.raw() {
            return Err(Error::StorageFull);
        }

        let file_page_id = FilePageId::from_raw(*next_id);
        *next_id += 1;

        // Track allocation in current file
        let file_id = *self.current_file_id.read().unwrap();
        let mut allocations = self.file_allocations.write().unwrap();
        allocations.entry(file_id)
            .or_insert_with(Vec::new)
            .push(file_page_id);

        Ok(file_page_id)
    }

    /// Map a logical page to a physical page
    pub fn map_page(&self, page_id: PageId, file_page_id: FilePageId) -> Result<()> {
        let file_id = *self.current_file_id.read().unwrap();
        let mapping = PageMapping {
            page_id,
            file_page_id,
            file_id,
        };

        let mut mappings = self.mappings.write().unwrap();
        mappings.insert(page_id, mapping);

        // Update version
        let mut version = self.version_counter.write().unwrap();
        *version += 1;

        Ok(())
    }

    /// Update mapping from manifest restore
    pub fn update_mapping(&self, page_id: PageId, file_page_id: FilePageId) {
        // Extract file_id from the file_page_id
        let file_id = file_page_id.file_id();

        let mapping = PageMapping {
            page_id,
            file_page_id,
            file_id: file_id as u64,
        };

        let mut mappings = self.mappings.write().unwrap();
        mappings.insert(page_id, mapping);

        // Update next_page_id if needed
        let mut next_id = self.next_page_id.write().unwrap();
        if page_id >= *next_id {
            *next_id = page_id + 1;
        }

        // Update version
        let mut version = self.version_counter.write().unwrap();
        *version += 1;
    }

    /// Allocate and map a new page
    pub fn allocate_and_map(&self) -> Result<(PageId, FilePageId)> {
        let page_id = self.allocate_page()?;
        let file_page_id = self.allocate_file_page()?;
        self.map_page(page_id, file_page_id)?;
        Ok((page_id, file_page_id))
    }

    /// Unmap a logical page
    pub fn unmap_page(&self, page_id: PageId) -> Result<()> {
        let mut mappings = self.mappings.write().unwrap();

        if mappings.remove(&page_id).is_none() {
            return Err(Error::NotFound);
        }

        // Update version
        let mut version = self.version_counter.write().unwrap();
        *version += 1;

        Ok(())
    }

    /// Get current snapshot
    pub fn snapshot(&self) -> MappingSnapshot {
        let mappings = self.mappings.read().unwrap();
        let version = *self.version_counter.read().unwrap();
        MappingSnapshot::from_mappings(mappings.clone(), version)
    }

    /// Restore mappings from a snapshot
    pub fn restore_from_snapshot(&self, snapshot: &MappingSnapshot) {
        let mut mappings = self.mappings.write().unwrap();
        mappings.clear();

        // Copy all mappings from snapshot
        for (page_id, mapping) in snapshot.all_mappings() {
            mappings.insert(*page_id, mapping.clone());
        }

        // Update counters based on snapshot
        let mut next_page = self.next_page_id.write().unwrap();
        *next_page = snapshot.max_page_id() + 1;

        // Update file page counter to the maximum found
        let mut max_file_page_id = 0u64;
        for mapping in mappings.values() {
            let raw = mapping.file_page_id.raw();
            if raw > max_file_page_id {
                max_file_page_id = raw;
            }
        }
        let mut next_file_page = self.next_file_page_id.write().unwrap();
        *next_file_page = max_file_page_id + 1;

        tracing::info!("Restored {} mappings from snapshot, next_page_id={}, next_file_page_id={}",
                      mappings.len(), *next_page, *next_file_page);
    }

    /// Export all mappings for manifest
    pub fn export_mappings(&self) -> Vec<(PageId, FilePageId)> {
        let mappings = self.mappings.read().unwrap();
        mappings.iter()
            .map(|(page_id, mapping)| (*page_id, mapping.file_page_id))
            .collect()
    }

    /// Restore from a snapshot
    pub fn restore(&self, snapshot: &MappingSnapshot) -> Result<()> {
        let mut mappings = self.mappings.write().unwrap();
        *mappings = snapshot.mappings.as_ref().clone();

        let mut next_page_id = self.next_page_id.write().unwrap();
        *next_page_id = snapshot.max_page_id + 1;

        // Update version
        let mut version = self.version_counter.write().unwrap();
        *version = snapshot.version + 1;

        Ok(())
    }

    /// Switch to a new file
    pub fn switch_file(&self, file_id: FileId) -> Result<()> {
        let mut current = self.current_file_id.write().unwrap();
        *current = file_id;
        Ok(())
    }

    /// Get current file ID
    pub fn current_file_id(&self) -> FileId {
        *self.current_file_id.read().unwrap()
    }

    /// Get the count of active mappings
    pub fn mapping_count(&self) -> usize {
        self.mappings.read().unwrap().len()
    }

    /// Get the total number of allocated pages
    pub fn allocated_pages(&self) -> usize {
        *self.next_page_id.read().unwrap() as usize
    }

    /// Get allocations for a specific file
    pub fn get_file_allocations(&self, file_id: FileId) -> Vec<FilePageId> {
        let allocations = self.file_allocations.read().unwrap();
        allocations.get(&file_id).cloned().unwrap_or_default()
    }

    /// Clear all mappings
    pub fn clear(&self) {
        let mut mappings = self.mappings.write().unwrap();
        mappings.clear();

        let mut next_page = self.next_page_id.write().unwrap();
        *next_page = 0;

        let mut next_file_page = self.next_file_page_id.write().unwrap();
        *next_file_page = 0;

        let mut allocations = self.file_allocations.write().unwrap();
        allocations.clear();

        let mut version = self.version_counter.write().unwrap();
        *version += 1;
    }

    /// Compact mappings (remove gaps in logical page IDs)
    pub fn compact(&self) -> Result<()> {
        let mut mappings = self.mappings.write().unwrap();

        if mappings.is_empty() {
            return Ok(());
        }

        let mut new_mappings = BTreeMap::new();
        let mut new_page_id = 0;

        // Reassign page IDs sequentially
        for (_, mapping) in mappings.iter() {
            let mut new_mapping = mapping.clone();
            new_mapping.page_id = new_page_id;
            new_mappings.insert(new_page_id, new_mapping);
            new_page_id += 1;
        }

        *mappings = new_mappings;

        let mut next_page = self.next_page_id.write().unwrap();
        *next_page = new_page_id;

        let mut version = self.version_counter.write().unwrap();
        *version += 1;

        Ok(())
    }
}

impl Default for PageMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_allocation() {
        let mapper = PageMapper::new();

        let page_id1 = mapper.allocate_page().unwrap();
        let page_id2 = mapper.allocate_page().unwrap();

        assert_eq!(page_id1, 0);
        assert_eq!(page_id2, 1);
    }

    #[test]
    fn test_page_mapping() {
        let mapper = PageMapper::new();

        let page_id = mapper.allocate_page().unwrap();
        let file_page_id = mapper.allocate_file_page().unwrap();

        mapper.map_page(page_id, file_page_id).unwrap();

        let snapshot = mapper.snapshot();
        assert_eq!(snapshot.get(page_id), Some(file_page_id));
    }

    #[test]
    fn test_allocate_and_map() {
        let mapper = PageMapper::new();

        let (page_id, file_page_id) = mapper.allocate_and_map().unwrap();

        let snapshot = mapper.snapshot();
        assert_eq!(snapshot.get(page_id), Some(file_page_id));
    }

    #[test]
    fn test_unmap_page() {
        let mapper = PageMapper::new();

        let (page_id, _) = mapper.allocate_and_map().unwrap();

        let snapshot = mapper.snapshot();
        assert!(snapshot.contains(page_id));

        mapper.unmap_page(page_id).unwrap();

        let snapshot = mapper.snapshot();
        assert!(!snapshot.contains(page_id));
    }

    #[test]
    fn test_snapshot_restore() {
        let mapper = PageMapper::new();

        // Create some mappings
        mapper.allocate_and_map().unwrap();
        mapper.allocate_and_map().unwrap();

        let snapshot1 = mapper.snapshot();
        assert_eq!(snapshot1.len(), 2);

        // Add more mappings
        mapper.allocate_and_map().unwrap();
        let snapshot2 = mapper.snapshot();
        assert_eq!(snapshot2.len(), 3);

        // Restore to earlier snapshot
        mapper.restore(&snapshot1).unwrap();
        let snapshot3 = mapper.snapshot();
        assert_eq!(snapshot3.len(), 2);
    }

    #[test]
    fn test_file_switching() {
        let mapper = PageMapper::new();

        assert_eq!(mapper.current_file_id(), 0);

        mapper.switch_file(5).unwrap();
        assert_eq!(mapper.current_file_id(), 5);

        let (_, file_page_id) = mapper.allocate_and_map().unwrap();

        let allocations = mapper.get_file_allocations(5);
        assert!(allocations.contains(&file_page_id));
    }

    #[test]
    fn test_compact() {
        let mapper = PageMapper::new();

        // Create mappings with gaps
        let page_id1 = mapper.allocate_page().unwrap();
        let file_page_id1 = mapper.allocate_file_page().unwrap();
        mapper.map_page(page_id1, file_page_id1).unwrap();

        let _ = mapper.allocate_page().unwrap(); // Gap

        let page_id3 = mapper.allocate_page().unwrap();
        let file_page_id3 = mapper.allocate_file_page().unwrap();
        mapper.map_page(page_id3, file_page_id3).unwrap();

        mapper.compact().unwrap();

        let snapshot = mapper.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains(0));
        assert!(snapshot.contains(1));
        assert!(!snapshot.contains(2));
    }
}