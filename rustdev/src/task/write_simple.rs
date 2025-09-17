/// Simplified write implementation for testing
/// This is a minimal implementation to demonstrate persistence

use bytes::Bytes;
use crate::error::Result;
use crate::page::{PageCache, DataPageBuilder, DataPageIterator, DataPage};
use crate::types::TableIdent;
use crate::storage::AsyncFileManager;
use crate::page::PageMapper;
use crate::index::IndexPageManager;
use std::sync::Arc;

pub async fn simple_write(
    key: &[u8],
    value: &[u8],
    table_id: &TableIdent,
    page_cache: Arc<PageCache>,
    page_mapper: Arc<PageMapper>,
    file_manager: Arc<AsyncFileManager>,
    index_manager: Arc<IndexPageManager>,
) -> Result<()> {
    tracing::debug!("Simple write: key={:?}, value={:?}",
                   String::from_utf8_lossy(key),
                   String::from_utf8_lossy(value));

    // For testing: create a simple page with the data
    // Get or allocate a page ID
    let page_id = 1u32; // Simple fixed page for testing

    // Use DataPageBuilder to create the page
    let mut builder = DataPageBuilder::new(4096);
    builder.add(key, value, 0, None, false);
    let data_page = builder.finish(page_id);

    // Write to disk through file manager
    // Ensure file exists first
    let file_id = if file_manager.get_metadata(0).await.is_err() {
        // File doesn't exist, create it
        file_manager.create_file(table_id).await?
    } else {
        0 // Use file 0
    };

    // Map the page to a file location
    page_mapper.switch_file(file_id)?;
    let file_page_id = page_mapper.allocate_file_page()?;
    page_mapper.map_page(page_id, file_page_id)?;

    tracing::debug!("Writing page {} to file page {}", page_id, file_page_id);

    file_manager.write_page(
        file_page_id.file_id() as u64,
        file_page_id.page_offset(),
        data_page.as_page()
    ).await?;

    // Also add to cache
    let data_page_arc = Arc::new(data_page);
    page_cache.insert(Bytes::copy_from_slice(key), data_page_arc).await;

    // Update index manager to track this root
    // For testing: create a simple COW meta
    use crate::index::CowRootMeta;
    let mut cow_meta = CowRootMeta::default();
    cow_meta.root_id = page_id;
    index_manager.update_root(table_id, cow_meta);

    tracing::debug!("Simple write completed");

    Ok(())
}

pub async fn simple_read(
    key: &[u8],
    table_id: &TableIdent,
    page_cache: Arc<PageCache>,
    page_mapper: Arc<PageMapper>,
    file_manager: Arc<AsyncFileManager>,
    index_manager: Arc<IndexPageManager>,
) -> Result<Option<Bytes>> {
    tracing::debug!("Simple read: key={:?}", String::from_utf8_lossy(key));

    // For testing: read from fixed page
    let page_id = 1u32;

    // Try cache first
    if let Some(data_page) = page_cache.get(&Bytes::copy_from_slice(key)).await {
        // Use iterator to find the key
        let mut iter = DataPageIterator::new(&data_page);
        let value = if iter.seek(key) {
            iter.value()
        } else {
            None
        };
        tracing::debug!("Found in cache: {:?}", value.as_ref().map(|v| String::from_utf8_lossy(v)));
        return Ok(value.map(Bytes::from));
    }

    // Read from disk
    let snapshot = page_mapper.snapshot();
    if let Some(file_page_id) = snapshot.get(page_id) {
        tracing::debug!("Reading page {} from file page {}", page_id, file_page_id);

        let page = file_manager.read_page(file_page_id.file_id() as u64, file_page_id.page_offset()).await?;

        // Parse the page as a DataPage
        let data_page = DataPage::from_page(page_id, page);

        // Add to cache
        let data_page_arc = Arc::new(data_page);
        page_cache.insert(Bytes::copy_from_slice(key), data_page_arc.clone()).await;

        let data_page = data_page_arc;
        // Use iterator to find the key
        let mut iter = DataPageIterator::new(&data_page);
        let value = if iter.seek(key) {
            iter.value()
        } else {
            None
        };
        tracing::debug!("Found on disk: {:?}", value.as_ref().map(|v| String::from_utf8_lossy(v)));
        return Ok(value.map(Bytes::from));
    }

    tracing::debug!("Key not found");
    Ok(None)
}