//! Data page codec following exact C++ format
//!
//! This module implements encoding and decoding of data page entries
//! in the exact format used by the C++ implementation.

use bytes::{Bytes, BytesMut, BufMut};
use crate::Result;

/// Value length special bits (must match C++ ValLenBit enum)
#[repr(u8)]
pub enum ValLenBit {
    Overflow = 0,
    Expire = 1,
    Reserved2 = 2,
    Reserved3 = 3,
    BitsCount = 4,
}

/// Encode a variable-length 32-bit integer (following C++ GetVarint32Ptr)
pub fn encode_varint32(value: u32) -> Vec<u8> {
    let mut result = Vec::with_capacity(5);
    let mut val = value;

    while val >= 0x80 {
        result.push((val as u8) | 0x80);
        val >>= 7;
    }
    result.push(val as u8);

    result
}

/// Encode varint32 into buffer
pub fn encode_varint32_into(buf: &mut impl BufMut, value: u32) {
    let mut val = value;
    while val >= 0x80 {
        buf.put_u8((val as u8) | 0x80);
        val >>= 7;
    }
    buf.put_u8(val as u8);
}

/// Decode a variable-length 32-bit integer
pub fn decode_varint32(data: &[u8]) -> Option<(u32, usize)> {
    let mut result = 0u32;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return None; // Varint32 too long
        }

        if shift >= 32 {
            return None;
        }

        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None
}

/// Encode a variable-length 64-bit integer (following C++ GetVarint64Ptr)
pub fn encode_varint64(value: u64) -> Vec<u8> {
    let mut result = Vec::with_capacity(10);
    let mut val = value;

    while val >= 0x80 {
        result.push((val as u8) | 0x80);
        val >>= 7;
    }
    result.push(val as u8);

    result
}

/// Encode varint64 into buffer
pub fn encode_varint64_into(buf: &mut impl BufMut, value: u64) {
    let mut val = value;
    while val >= 0x80 {
        buf.put_u8((val as u8) | 0x80);
        val >>= 7;
    }
    buf.put_u8(val as u8);
}

/// Decode a variable-length 64-bit integer
pub fn decode_varint64(data: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            return None; // Varint64 too long
        }

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None
}

/// Encode int64 delta (following C++ EncodeInt64Delta)
pub fn encode_int64_delta(delta: i64) -> u64 {
    // C++ implementation: zigzag encoding
    if delta < 0 {
        ((!delta as u64) << 1) | 1
    } else {
        (delta as u64) << 1
    }
}

/// Decode int64 delta (following C++ DecodeInt64Delta)
pub fn decode_int64_delta(encoded: u64) -> i64 {
    // C++ implementation: zigzag decoding
    if encoded & 1 != 0 {
        !(encoded >> 1) as i64
    } else {
        (encoded >> 1) as i64
    }
}

/// Data page entry encoder (following C++ format exactly)
pub struct DataPageEncoder {
    buffer: BytesMut,
    last_timestamp: u64,
}

impl DataPageEncoder {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(256),
            last_timestamp: 0,
        }
    }

    /// Reset for a new page
    pub fn reset(&mut self) {
        self.last_timestamp = 0;
    }

    /// Encode a data entry following C++ format
    ///
    /// C++ format (from DataPageIter::DecodeEntry):
    /// - shared_key_len: varint32 (for prefix compression, 0 for restart points)
    /// - non_shared_key_len: varint32
    /// - value_length_with_flags: varint32 (flags in lower bits)
    /// - key data (non-shared portion)
    /// - value data
    /// - expire_ts: varint64 (if expire flag set)
    /// - timestamp_delta: varint64 (encoded as zigzag)
    pub fn encode_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
        expire_ts: Option<u64>,
        is_overflow: bool,
        is_restart: bool,
        last_key: Option<&[u8]>,
    ) -> Bytes {
        self.buffer.clear();

        // Calculate shared prefix length (0 for restart points)
        let shared_len = if is_restart || last_key.is_none() {
            0
        } else {
            let last = last_key.unwrap();
            let min_len = key.len().min(last.len());
            let mut shared = 0;
            for i in 0..min_len {
                if key[i] != last[i] {
                    break;
                }
                shared += 1;
            }
            shared
        };

        let non_shared_len = key.len() - shared_len;

        // Fast path for small values (following C++ optimization)
        if shared_len < 128 && non_shared_len < 128 && value.len() < (128 >> ValLenBit::BitsCount as u8) {
            // All three values fit in one byte each
            self.buffer.put_u8(shared_len as u8);
            self.buffer.put_u8(non_shared_len as u8);

            // Pack value length with flags
            let mut val_len_byte = (value.len() as u8) << ValLenBit::BitsCount as u8;
            if is_overflow {
                val_len_byte |= 1 << ValLenBit::Overflow as u8;
            }
            if expire_ts.is_some() {
                val_len_byte |= 1 << ValLenBit::Expire as u8;
            }
            self.buffer.put_u8(val_len_byte);
        } else {
            // Use varint encoding
            encode_varint32_into(&mut self.buffer, shared_len as u32);
            encode_varint32_into(&mut self.buffer, non_shared_len as u32);

            // Pack value length with flags
            let mut val_len_with_flags = (value.len() as u32) << ValLenBit::BitsCount as u8;
            if is_overflow {
                val_len_with_flags |= 1 << ValLenBit::Overflow as u8;
            }
            if expire_ts.is_some() {
                val_len_with_flags |= 1 << ValLenBit::Expire as u8;
            }
            encode_varint32_into(&mut self.buffer, val_len_with_flags);
        }

        // Write key data (only non-shared portion)
        self.buffer.put_slice(&key[shared_len..]);

        // Write value data
        self.buffer.put_slice(value);

        // Write expire timestamp if present
        if let Some(expire) = expire_ts {
            encode_varint64_into(&mut self.buffer, expire);
        }

        // Calculate and write timestamp delta
        let timestamp_value = if is_restart {
            // For restart points, store absolute timestamp as signed value
            encode_int64_delta(timestamp as i64)
        } else {
            // For other entries, store delta from last timestamp
            let delta = timestamp as i64 - self.last_timestamp as i64;
            encode_int64_delta(delta)
        };
        encode_varint64_into(&mut self.buffer, timestamp_value);

        // Update last timestamp
        self.last_timestamp = timestamp;

        self.buffer.clone().freeze()
    }
}

/// Decode a data page entry (following C++ DataPageIter::DecodeEntry)
pub fn decode_entry(
    data: &[u8],
    offset: usize,
) -> Option<(u32, u32, u32, bool, bool, usize)> {
    let mut pos = offset;

    // Decode shared key length
    let (shared, bytes_read) = decode_varint32(&data[pos..])?;
    pos += bytes_read;

    // Decode non-shared key length
    let (non_shared, bytes_read) = decode_varint32(&data[pos..])?;
    pos += bytes_read;

    // Decode value length with flags
    let (val_len_with_flags, bytes_read) = decode_varint32(&data[pos..])?;
    pos += bytes_read;

    // Extract flags
    let overflow = (val_len_with_flags & (1 << ValLenBit::Overflow as u8)) != 0;
    let expire = (val_len_with_flags & (1 << ValLenBit::Expire as u8)) != 0;
    let value_length = val_len_with_flags >> ValLenBit::BitsCount as u8;

    // Return decoded values and new position
    Some((shared, non_shared, value_length, overflow, expire, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint32_encode_decode() {
        let values = vec![0, 127, 128, 255, 256, 65535, 65536, u32::MAX];
        for val in values {
            let encoded = encode_varint32(val);
            let (decoded, size) = decode_varint32(&encoded).unwrap();
            assert_eq!(val, decoded);
            assert_eq!(size, encoded.len());
        }
    }

    #[test]
    fn test_varint64_encode_decode() {
        let values = vec![0, 127, 128, 255, 256, 65535, 65536, u32::MAX as u64, u64::MAX];
        for val in values {
            let encoded = encode_varint64(val);
            let (decoded, size) = decode_varint64(&encoded).unwrap();
            assert_eq!(val, decoded);
            assert_eq!(size, encoded.len());
        }
    }

    #[test]
    fn test_int64_delta_encode_decode() {
        let values = vec![0, 1, -1, 127, -128, i64::MAX, i64::MIN];
        for val in values {
            let encoded = encode_int64_delta(val);
            let decoded = decode_int64_delta(encoded);
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_entry_encoding() {
        let mut encoder = DataPageEncoder::new();

        // Test encoding a restart point
        let encoded = encoder.encode_entry(
            b"key1",
            b"value1",
            1000,
            Some(2000),
            false,
            true,  // is_restart
            None,
        );

        // Verify we can decode it
        let (shared, non_shared, val_len, overflow, expire, _pos) =
            decode_entry(&encoded, 0).unwrap();

        assert_eq!(shared, 0); // Restart points have 0 shared
        assert_eq!(non_shared, 4); // "key1" length
        assert_eq!(val_len, 6); // "value1" length
        assert_eq!(overflow, false);
        assert_eq!(expire, true);
    }
}