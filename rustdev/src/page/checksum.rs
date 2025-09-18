//! Checksum calculation matching C++ implementation
//!
//! IMPORTANT: C++ uses XXH3_64bits for checksums, not CRC32C!
//! This module provides compatible checksum calculation.

use xxhash_rust::xxh3::xxh3_64;

/// Calculate checksum for page data (following C++ SetChecksum)
///
/// C++ implementation:
/// ```cpp
/// uint64_t checksum = XXH3_64bits(blob.data() + checksum_bytes, blob.size() - checksum_bytes);
/// ```
pub fn calculate_checksum(data: &[u8], checksum_offset: usize) -> u64 {
    // Skip the checksum bytes and hash the rest
    if data.len() > checksum_offset {
        xxh3_64(&data[checksum_offset..])
    } else {
        0
    }
}

/// Set checksum in page data (following C++ SetChecksum)
pub fn set_checksum(data: &mut [u8]) {
    const CHECKSUM_BYTES: usize = 8;

    if data.len() <= CHECKSUM_BYTES {
        return;
    }

    // Calculate checksum for everything after the checksum field
    let checksum = xxh3_64(&data[CHECKSUM_BYTES..]);

    // Write checksum as little-endian at the beginning
    data[0..8].copy_from_slice(&checksum.to_le_bytes());
}

/// Validate checksum in page data (following C++ ValidateChecksum)
pub fn validate_checksum(data: &[u8]) -> bool {
    const CHECKSUM_BYTES: usize = 8;

    if data.len() <= CHECKSUM_BYTES {
        return false;
    }

    // Extract stored checksum
    let stored = u64::from_le_bytes(data[0..8].try_into().unwrap());

    // Calculate actual checksum
    let calculated = xxh3_64(&data[CHECKSUM_BYTES..]);

    stored == calculated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_calculation() {
        let mut data = vec![0u8; 100];

        // Fill with test pattern
        for i in 8..100 {
            data[i] = (i % 256) as u8;
        }

        // Set checksum
        set_checksum(&mut data);

        // Validate it
        assert!(validate_checksum(&data));

        // Corrupt data
        data[50] = 255;

        // Should fail validation
        assert!(!validate_checksum(&data));
    }

    #[test]
    fn test_empty_data() {
        let data = vec![0u8; 4];
        assert!(!validate_checksum(&data));
    }
}