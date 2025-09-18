//! Data page iterator that reads C++ compatible format
//!
//! This iterator can read data pages encoded in the C++ format
//! with prefix compression and delta timestamps.

use bytes::Bytes;
use std::cmp::Ordering;

use crate::codec::data_page_codec::{decode_entry, decode_varint32, decode_varint64, decode_int64_delta};
use crate::types::PageId;
use crate::page::HEADER_SIZE;
use super::data_page::DataPage;

/// Iterator over entries in a data page (C++ compatible)
pub struct DataPageIterator {
    /// The data page being iterated
    page: DataPage,
    /// Current position in the page
    current_pos: usize,
    /// End position (where restart array begins)
    end_pos: usize,
    /// Current key (accumulated from shared prefixes)
    current_key: Vec<u8>,
    /// Current value
    current_value: Option<Bytes>,
    /// Current timestamp
    current_timestamp: u64,
    /// Current expire timestamp
    current_expire: Option<u64>,
    /// Is current entry an overflow
    is_overflow: bool,
    /// Current restart index
    current_restart_idx: usize,
}

impl DataPageIterator {
    /// Create a new iterator for a data page
    pub fn new(page: DataPage) -> Self {
        let content_len = page.content_length() as usize;
        let end_pos = HEADER_SIZE + content_len;

        Self {
            page,
            current_pos: HEADER_SIZE,
            end_pos,
            current_key: Vec::new(),
            current_value: None,
            current_timestamp: 0,
            current_expire: None,
            is_overflow: false,
            current_restart_idx: 0,
        }
    }

    /// Check if iterator has more entries
    pub fn has_next(&self) -> bool {
        self.current_pos < self.end_pos
    }

    /// Move to next entry
    pub fn next(&mut self) -> bool {
        if !self.has_next() {
            return false;
        }

        // Try to parse the next entry
        if let Some((key, value, timestamp, expire)) = self.parse_next_entry() {
            self.current_key = key;
            self.current_value = Some(Bytes::from(value));
            self.current_timestamp = timestamp;
            self.current_expire = expire;
            true
        } else {
            false
        }
    }

    /// Get current key
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Get current value
    pub fn value(&self) -> Option<&Bytes> {
        self.current_value.as_ref()
    }

    /// Get current timestamp
    pub fn timestamp(&self) -> u64 {
        self.current_timestamp
    }

    /// Get current expiration
    pub fn expire_ts(&self) -> Option<u64> {
        self.current_expire
    }

    /// Check if current entry is overflow
    pub fn is_overflow(&self) -> bool {
        self.is_overflow
    }

    /// Parse the next entry from the page
    fn parse_next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>, u64, Option<u64>)> {
        if self.current_pos >= self.end_pos {
            return None;
        }

        let data = self.page.page().as_bytes();
        let mut pos = self.current_pos;

        // Check if this is a restart point
        let is_restart = self.is_restart_point();

        // Decode entry header (shared, non_shared, value_len with flags)
        let (shared, non_shared, value_len, overflow, expire, new_pos) =
            decode_entry(data, pos)?;
        pos = new_pos;

        // Build the key
        let mut key = Vec::with_capacity(shared as usize + non_shared as usize);

        // Copy shared prefix from current key
        if shared > 0 && !is_restart {
            if self.current_key.len() >= shared as usize {
                key.extend_from_slice(&self.current_key[..shared as usize]);
            } else {
                // Invalid shared length
                return None;
            }
        }

        // Read non-shared portion of key
        if pos + non_shared as usize > data.len() {
            return None;
        }
        key.extend_from_slice(&data[pos..pos + non_shared as usize]);
        pos += non_shared as usize;

        // Read value
        if pos + value_len as usize > data.len() {
            return None;
        }
        let value = data[pos..pos + value_len as usize].to_vec();
        pos += value_len as usize;

        // Read expire timestamp if present
        let expire_ts = if expire {
            let (expire_val, bytes_read) = decode_varint64(&data[pos..])?;
            pos += bytes_read;
            Some(expire_val)
        } else {
            None
        };

        // Read timestamp delta
        let (encoded_ts, bytes_read) = decode_varint64(&data[pos..])?;
        pos += bytes_read;

        let timestamp = if is_restart {
            // For restart points, timestamp is stored as absolute value
            decode_int64_delta(encoded_ts) as u64
        } else {
            // For other entries, it's a delta from previous timestamp
            let delta = decode_int64_delta(encoded_ts);
            (self.current_timestamp as i64 + delta) as u64
        };

        // Update position
        self.current_pos = pos;

        // Store overflow flag
        self.is_overflow = overflow;

        Some((key, value, timestamp, expire_ts))
    }

    /// Check if current position is at a restart point
    fn is_restart_point(&self) -> bool {
        // Check restart points to see if current position matches
        let restart_count = self.page.restart_num();
        for i in 0..restart_count {
            if let Some(offset) = self.page.restart_point(i) {
                if self.current_pos == HEADER_SIZE + offset as usize {
                    return true;
                }
            }
        }
        false
    }

    /// Seek to the first key >= target
    pub fn seek(&mut self, target_key: &[u8]) -> bool {
        // Reset to beginning
        self.current_pos = HEADER_SIZE;
        self.current_key.clear();
        self.current_timestamp = 0;

        // Use binary search on restart points if available
        let restart_count = self.page.restart_num();
        if restart_count == 0 {
            return false;
        }

        // Find the best restart point to start from
        let mut best_restart = 0;
        for i in 0..restart_count {
            if let Some(offset) = self.page.restart_point(i) {
                let saved_pos = self.current_pos;
                let saved_key = self.current_key.clone();
                let saved_ts = self.current_timestamp;

                self.current_pos = HEADER_SIZE + offset as usize;

                if let Some((key, _, _, _)) = self.parse_next_entry() {
                    if key.as_slice() <= target_key {
                        best_restart = i;
                    } else {
                        // Restore state and break
                        self.current_pos = saved_pos;
                        self.current_key = saved_key;
                        self.current_timestamp = saved_ts;
                        break;
                    }
                }
            }
        }

        // Start from the best restart point
        if let Some(offset) = self.page.restart_point(best_restart) {
            self.current_pos = HEADER_SIZE + offset as usize;
            self.current_key.clear();
            self.current_timestamp = 0;
        }

        // Linear search from the restart point
        while self.next() {
            if self.key() >= target_key {
                return true;
            }
        }

        false
    }

    /// Reset the iterator to the beginning
    pub fn reset(&mut self) {
        self.current_pos = HEADER_SIZE;
        self.current_key.clear();
        self.current_value = None;
        self.current_timestamp = 0;
        self.current_expire = None;
        self.is_overflow = false;
        self.current_restart_idx = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::data_page_builder::DataPageBuilder;

    #[test]
    fn test_iterator_basic() {
        // Create a page with some entries
        let mut builder = DataPageBuilder::new(4096);

        builder.add(b"key1", b"value1", 1000, None, false);
        builder.add(b"key2", b"value2", 2000, None, false);
        builder.add(b"key3", b"value3", 3000, Some(4000), false);

        let page = builder.finish(1);

        // Iterate through entries
        let mut iter = DataPageIterator::new(page);

        assert!(iter.next());
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value().unwrap().as_ref(), b"value1");
        assert_eq!(iter.timestamp(), 1000);
        assert_eq!(iter.expire_ts(), None);

        assert!(iter.next());
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value().unwrap().as_ref(), b"value2");
        assert_eq!(iter.timestamp(), 2000);

        assert!(iter.next());
        assert_eq!(iter.key(), b"key3");
        assert_eq!(iter.value().unwrap().as_ref(), b"value3");
        assert_eq!(iter.timestamp(), 3000);
        assert_eq!(iter.expire_ts(), Some(4000));

        assert!(!iter.next());
    }

    #[test]
    fn test_iterator_seek() {
        let mut builder = DataPageBuilder::new(4096);

        builder.add(b"apple", b"value1", 1000, None, false);
        builder.add(b"banana", b"value2", 2000, None, false);
        builder.add(b"cherry", b"value3", 3000, None, false);
        builder.add(b"date", b"value4", 4000, None, false);

        let page = builder.finish(1);
        let mut iter = DataPageIterator::new(page);

        // Seek to "banana"
        assert!(iter.seek(b"banana"));
        assert_eq!(iter.key(), b"banana");

        // Seek to "blueberry" (should find "cherry")
        iter.reset();
        assert!(iter.seek(b"blueberry"));
        assert_eq!(iter.key(), b"cherry");

        // Seek to "zebra" (should not find)
        iter.reset();
        assert!(!iter.seek(b"zebra"));
    }
}