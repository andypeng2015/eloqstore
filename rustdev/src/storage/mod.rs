//! Storage backend abstraction and implementations

pub mod file_manager;
pub mod async_file_manager;
pub mod manifest;
pub mod traits;

pub use file_manager::{FileManager, FileMetadata};
pub use async_file_manager::{AsyncFileManager, AsyncFileMetadata};
pub use manifest::{ManifestBuilder, ManifestData, ManifestFile, ManifestReplayer, create_archive};
pub use traits::*;