//! Utility functions and data structures

pub mod comparator;

pub use comparator::{Comparator, BytewiseComparator, DEFAULT_COMPARATOR};

/// Calculate CRC32 checksum
pub fn crc32(data: &[u8]) -> u64 {
    // Use a simple CRC32 implementation or crate
    // For now, return a placeholder
    // TODO: Use proper CRC32 implementation
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}