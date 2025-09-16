//! Key comparator trait and implementations
//! Following C++ comparator.h

use std::cmp::Ordering;
use std::fmt::Debug;

/// Comparator trait for key comparison
pub trait Comparator: Send + Sync + Debug {
    /// Compare two keys
    fn compare(&self, a: &[u8], b: &[u8]) -> i8;

    /// Get comparator name
    fn name(&self) -> &str;

    /// Find shortest separator between start and limit
    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        // Default implementation: no change
    }

    /// Find short successor of key
    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Default implementation: no change
    }
}

/// Bytewise comparator (default)
#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl BytewiseComparator {
    pub fn new() -> Self {
        Self
    }
}

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> i8 {
        match a.cmp(b) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        // Find the first differing byte
        let min_len = start.len().min(limit.len());
        let mut diff_index = 0;

        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index >= min_len {
            // One string is a prefix of the other
            return;
        }

        // Try to increment the differing byte
        let diff_byte = start[diff_index];
        if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
            start.truncate(diff_index + 1);
            start[diff_index] += 1;
        }
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Find the first byte that can be incremented
        for i in 0..key.len() {
            if key[i] != 0xff {
                key.truncate(i + 1);
                key[i] += 1;
                return;
            }
        }
        // No successor possible
    }
}

/// Reverse bytewise comparator
#[derive(Debug, Clone)]
pub struct ReverseBytewiseComparator;

impl ReverseBytewiseComparator {
    pub fn new() -> Self {
        Self
    }
}

impl Comparator for ReverseBytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> i8 {
        match b.cmp(a) {  // Note: reversed
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    fn name(&self) -> &str {
        "leveldb.ReverseBytewiseComparator"
    }
}