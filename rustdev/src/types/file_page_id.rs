//! File page ID implementation following C++ structure
//! FilePageId encodes both file ID and page offset within the file

use std::fmt;
use serde::{Serialize, Deserialize};

/// File page identifier that encodes both file ID and page offset
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FilePageId(u64);

impl FilePageId {
    /// Create a new file page ID
    pub fn new(file_id: u32, page_offset: u32) -> Self {
        // Encode as: high 32 bits = file_id, low 32 bits = page_offset
        Self(((file_id as u64) << 32) | (page_offset as u64))
    }

    /// Get the file ID portion
    pub fn file_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Get the page offset within the file
    pub fn page_offset(&self) -> u64 {
        (self.0 & 0xFFFFFFFF) as u64
    }

    /// Get raw value
    pub fn raw(&self) -> u64 {
        self.0
    }

    /// Convert to little-endian bytes
    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Create from raw value
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Check if valid
    pub fn is_valid(&self) -> bool {
        self.0 != u64::MAX
    }
}

impl From<u64> for FilePageId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<FilePageId> for u64 {
    fn from(fp: FilePageId) -> Self {
        fp.0
    }
}

impl fmt::Display for FilePageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilePageId(file={}, offset={})", self.file_id(), self.page_offset())
    }
}

/// Maximum valid file page ID
pub const MAX_FILE_PAGE_ID: FilePageId = FilePageId(u64::MAX);