//! Response types for database operations

use bytes::Bytes;

/// Response types
#[derive(Debug)]
pub enum Response {
    /// Read response
    Read(ReadResponse),
    /// Floor response
    Floor(FloorResponse),
    /// Scan response
    Scan(ScanResponse),
    /// Batch write response
    BatchWrite(BatchWriteResponse),
    /// Truncate response
    Truncate(TruncateResponse),
    /// Archive response
    Archive(ArchiveResponse),
    /// Compact response
    Compact(CompactResponse),
    /// Clean expired response
    CleanExpired(CleanExpiredResponse),
}

/// Read response
#[derive(Debug)]
pub struct ReadResponse {
    pub value: Option<crate::types::Value>,
}

/// Floor response
#[derive(Debug)]
pub struct FloorResponse {
    pub key: Bytes,
    pub value: Bytes,
    pub timestamp: u64,
    pub expire_ts: Option<u64>,
}

/// Scan response
#[derive(Debug)]
pub struct ScanResponse {
    pub entries: Vec<(crate::types::Key, crate::types::Value)>,
    pub has_more: bool,
}

/// A single entry in a scan result
#[derive(Debug)]
pub struct ScanEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub timestamp: u64,
    pub expire_ts: Option<u64>,
}

/// Batch write response
#[derive(Debug)]
pub struct BatchWriteResponse {
    pub success: bool,
}

/// Write response
#[derive(Debug)]
pub struct WriteResponse {
    pub success: bool,
}

/// Delete response
#[derive(Debug)]
pub struct DeleteResponse {
    pub success: bool,
}

/// Truncate response
#[derive(Debug)]
pub struct TruncateResponse {
    pub deleted_count: u64,
}

/// Archive response
#[derive(Debug)]
pub struct ArchiveResponse {
    pub archived_count: u64,
    pub archive_path: String,
}

/// Compact response
#[derive(Debug)]
pub struct CompactResponse {
    pub compacted_files: usize,
    pub space_reclaimed: u64,
}

/// Clean expired response
#[derive(Debug)]
pub struct CleanExpiredResponse {
    pub cleaned_count: u64,
}