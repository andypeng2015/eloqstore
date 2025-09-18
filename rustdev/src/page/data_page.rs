//! Data page implementation with restart points for efficient binary search

use bytes::Bytes;
use std::cmp::Ordering;

use crate::types::{PageId, PageType, MAX_PAGE_ID};
use crate::{Result, Error};
use super::page::{Page, HEADER_SIZE};

/// Restart point for binary search
#[derive(Debug, Clone, Copy)]
pub struct RestartPoint {
    /// Offset in page data
    pub offset: u32,
    /// Key length
    pub key_len: u32,
}

/// Value length special bits
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ValueLenBit {
    /// Value is overflow (stored in overflow pages)
    Overflow = 0,
    /// Entry has expiration timestamp
    Expire = 1,
    /// Reserved for future use
    Reserved2 = 2,
    /// Reserved for future use
    Reserved3 = 3,
}

/// Data page structure
///
/// Format:
/// ```text
/// +------------+--------+------------------+-------------+-------------+
/// |checksum(8B)|type(1B)|content length(2B)|prev page(4B)|next page(4B)|
/// +------------+--------+------------------+-------------+-------------+
/// +---------+-------------------+---------------+-------------+
/// |data blob|restart array(N*2B)|restart num(2B)|padding bytes|
/// +---------+-------------------+---------------+-------------+
/// ```
#[derive(Debug, Clone)]
pub struct DataPage {
    /// Page ID
    page_id: PageId,
    /// Underlying page
    page: Page,
}

impl DataPage {
    /// Create a new empty data page
    pub fn new(page_id: PageId, page_size: usize) -> Self {
        let mut page = Page::new(page_size);
        page.set_page_type(PageType::Data);
        page.set_prev_page_id(MAX_PAGE_ID);
        page.set_next_page_id(MAX_PAGE_ID);
        page.set_content_length(0);

        // Initialize restart array
        let restart_num_offset = page_size - 2;
        page.as_bytes_mut()[restart_num_offset..restart_num_offset + 2]
            .copy_from_slice(&0u16.to_le_bytes());

        Self { page_id, page }
    }

    /// Create from an existing page
    pub fn from_page(page_id: PageId, page: Page) -> Self {
        Self { page_id, page }
    }

    /// Get page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Set page ID
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    /// Get previous page ID
    pub fn prev_page_id(&self) -> PageId {
        self.page.prev_page_id()
    }

    /// Set previous page ID
    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.page.set_prev_page_id(page_id);
    }

    /// Get next page ID
    pub fn next_page_id(&self) -> PageId {
        self.page.next_page_id()
    }

    /// Set next page ID
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.page.set_next_page_id(page_id);
    }

    /// Get content length
    pub fn content_length(&self) -> u16 {
        self.page.content_length()
    }

    /// Get number of restart points (following C++ RestartNum)
    pub fn restart_num(&self) -> u16 {
        self.restart_count()
    }

    /// Get number of restart points
    pub fn restart_count(&self) -> u16 {
        let offset = self.page.size() - 2;
        u16::from_le_bytes(
            self.page.as_bytes()[offset..offset + 2]
                .try_into()
                .unwrap()
        )
    }

    /// Get restart point at index
    pub fn restart_point(&self, index: u16) -> Option<u16> {
        if index >= self.restart_count() {
            return None;
        }

        let restart_array_offset = self.page.size() - 2 - (self.restart_count() as usize * 2);
        let offset = restart_array_offset + (index as usize * 2);

        Some(u16::from_le_bytes(
            self.page.as_bytes()[offset..offset + 2]
                .try_into()
                .unwrap()
        ))
    }

    /// Check if page is empty
    pub fn is_empty(&self) -> bool {
        self.content_length() == 0
    }

    /// Clear the page
    pub fn clear(&mut self) {
        self.page.clear();
        // Reset restart count
        let restart_num_offset = self.page.size() - 2;
        self.page.as_bytes_mut()[restart_num_offset..restart_num_offset + 2]
            .copy_from_slice(&0u16.to_le_bytes());
    }

    /// Get underlying page
    pub fn page(&self) -> &Page {
        &self.page
    }

    /// Get mutable underlying page
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    /// Take ownership of underlying page
    pub fn into_page(self) -> Page {
        self.page
    }

} // End of first impl DataPage block



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_page_creation() {
        let page = DataPage::new(123, 4096);
        assert_eq!(page.page_id(), 123);
        assert!(page.is_empty());
        assert_eq!(page.restart_count(), 0);
    }

}

impl DataPage {
    /// Get entry by key (following C++ Seek logic)
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());

        // Use binary search to find the key
        if iter.seek(key) {
            // Check if we found exact match
            if iter.key() == key {
                if let Some(value) = iter.value() {
                    return Ok(Some(value.to_vec()));
                }
            }
        }

        Ok(None)
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        // Note: In C++, insertion happens during page building, not on existing pages
        // Data pages are immutable once built
        Err(Error::NotSupported("DataPage is immutable after building".to_string()))
    }

    /// Check if key-value can fit
    pub fn can_fit(&self, key: &[u8], value: &[u8]) -> bool {
        // Size check - C++ doesn't have explicit can_fit, checks inline during append
        let entry_size = 4 + key.len() + 4 + value.len();
        let current_size = HEADER_SIZE + self.content_length() as usize;
        current_size + entry_size < self.page.size() - 100
    }

    /// Iterator over entries
    pub fn iter(&self) -> super::data_page_iterator::DataPageIterator {
        super::data_page_iterator::DataPageIterator::new(self.clone())
    }

    /// Find floor entry (largest key <= target) (following C++ SeekFloor)
    pub fn floor(&self, key: &[u8]) -> Result<Option<(Bytes, Bytes)>> {
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());

        // Use binary search to find floor
        if iter.seek(key) {
            if let Some(value) = iter.value() {
                return Ok(Some((Bytes::from(iter.key().to_vec()), value.clone())));
            }
        }

        Ok(None)
    }

    /// Get last entry
    pub fn last_entry(&self) -> Result<Option<(Bytes, Bytes)>> {
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());
        let mut last_entry = None;

        // Iterate through all entries to find the last one
        while iter.next() {
            if let Some(value) = iter.value() {
                last_entry = Some((Bytes::from(iter.key().to_vec()), value.clone()));
            }
        }

        Ok(last_entry)
    }

    /// Get first key
    pub fn first_key(&self) -> Option<Bytes> {
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());
        if iter.next() {
            Some(Bytes::from(iter.key().to_vec()))
        } else {
            None
        }
    }

    /// Get last key
    pub fn last_key(&self) -> Option<Bytes> {
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());
        let mut last_key = None;

        while iter.next() {
            last_key = Some(Bytes::from(iter.key().to_vec()));
        }

        last_key
    }

    /// Get entry count
    pub fn entry_count(&self) -> usize {
        // Count entries by iterating through the page
        let mut count = 0;
        let mut iter = super::data_page_iterator::DataPageIterator::new(self.clone());
        while iter.next() {
            count += 1;
        }
        count
    }

    /// Get used space
    pub fn used_space(&self) -> usize {
        HEADER_SIZE + self.content_length() as usize
    }

    /// Get as page reference
    pub fn as_page(&self) -> &Page {
        &self.page
    }

    /// Get size
    pub fn size(&self) -> usize {
        self.page.size()
    }
}
