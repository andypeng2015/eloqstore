//! Simple, user-friendly interface for EloqStore
//! Provides familiar get/set/delete operations with both sync and async variants

use std::sync::Arc;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::config::{Config, KvOptions};
use crate::error::Error;
use crate::Result;
use crate::types::TableIdent;
use crate::store::EloqStore;

use super::request::{ReadRequest, BatchWriteRequest, ScanRequest, WriteEntry, WriteOp};

/// A promise/future for async operations
pub struct AsyncResult<T> {
    receiver: oneshot::Receiver<Result<T>>,
}

impl<T> AsyncResult<T> {
    /// Wait for the result (blocking)
    pub fn wait(self) -> Result<T> {
        tokio::runtime::Handle::current()
            .block_on(self.receiver)
            .map_err(|_| Error::InvalidState("Async operation cancelled".into()))?
    }

    /// Get the result if ready (non-blocking)
    pub fn try_get(&mut self) -> Option<Result<T>> {
        match self.receiver.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                Some(Err(Error::InvalidState("Async operation cancelled".into())))
            }
        }
    }

    /// Convert to a future
    pub async fn get(self) -> Result<T> {
        self.receiver.await
            .map_err(|_| Error::InvalidState("Async operation cancelled".into()))?
    }
}

/// Simple store interface with familiar get/set operations
pub struct Store {
    store: Arc<EloqStore>,
    runtime: tokio::runtime::Handle,
}

impl Store {
    /// Create a new Store from Config
    pub async fn new(config: Config) -> Result<Self> {
        // For now, we'll just create a default KvOptions
        // Later we can implement proper mapping from Config to KvOptions
        let mut options = KvOptions::default();

        // Map basic config to options
        options.num_threads = config.runtime.num_shards as usize;
        options.data_dirs = config.storage.data_dirs.clone();
        options.data_page_size = config.storage.page_size;
        options.data_append_mode = config.storage.append_mode;

        Self::with_options(options)
    }

    /// Create a new Store from a directory path
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut options = KvOptions::default();
        options.data_dirs = vec![path.as_ref().to_path_buf()];
        let mut store = Self::with_options(options)?;
        store.start().await?;
        Ok(store)
    }

    /// Create a new Store with default options
    pub fn new_default() -> Result<Self> {
        Self::with_options(KvOptions::default())
    }

    /// Create a new Store with custom options
    pub fn with_options(options: KvOptions) -> Result<Self> {
        let store = Arc::new(EloqStore::new(options)?);
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // If not in async context, create a new runtime
                // This should not happen in normal usage
                return Err(Error::InvalidState("Must be called from within a tokio runtime".into()));
            }
        };

        Ok(Self {
            store,
            runtime,
        })
    }

    /// Start the store (must be called before any operations)
    pub async fn start(&self) -> Result<()> {
        // We need to get mutable access temporarily
        // This is safe because start() is only called once at initialization
        let store = unsafe {
            &mut *(Arc::as_ptr(&self.store) as *mut EloqStore)
        };
        store.start().await
    }

    /// Stop the store gracefully
    pub async fn shutdown(&self) -> Result<()> {
        // EloqStore doesn't have shutdown, use stop instead
        let store = unsafe {
            &mut *(Arc::as_ptr(&self.store) as *mut EloqStore)
        };
        store.stop().await
    }

    // ========== Synchronous Operations ==========
    // These use blocking operations and should only be called from non-async contexts
    // or from within spawn_blocking

    /// Get a value by key (blocking - use from sync context or spawn_blocking)
    pub fn get(&self, table: &str, key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();

        // Use Handle::block_on which works from sync context
        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::get_async_internal_static(store, &table, key).await
            })
        })
    }

    /// Set a key-value pair (blocking - use from sync context or spawn_blocking)
    pub fn set(&self, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();
        let value = value.into();

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::set_async_internal_static(store, &table, key, value).await
            })
        })
    }

    /// Delete a key (blocking - use from sync context or spawn_blocking)
    pub fn delete(&self, table: &str, key: impl Into<Bytes>) -> Result<()> {
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::delete_async_internal_static(store, &table, key).await
            })
        })
    }

    /// Get multiple values by keys (blocking - use from sync context or spawn_blocking)
    pub fn batch_get(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<Vec<Option<Bytes>>> {
        let store = self.store.clone();
        let table = table.to_string();
        let keys: Vec<Bytes> = keys.into_iter().map(|k| k.into()).collect();

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::batch_get_async_internal_static(store, &table, keys).await
            })
        })
    }

    /// Set multiple key-value pairs (blocking - use from sync context or spawn_blocking)
    pub fn batch_set(&self, table: &str, kvs: Vec<(impl Into<Bytes>, impl Into<Bytes>)>) -> Result<()> {
        let store = self.store.clone();
        let table = table.to_string();
        let kvs: Vec<(Bytes, Bytes)> = kvs.into_iter().map(|(k, v)| (k.into(), v.into())).collect();

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::batch_set_async_internal_static(store, &table, kvs).await
            })
        })
    }

    /// Delete multiple keys (blocking - use from sync context or spawn_blocking)
    pub fn batch_delete(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<()> {
        let store = self.store.clone();
        let table = table.to_string();
        let keys: Vec<Bytes> = keys.into_iter().map(|k| k.into()).collect();

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::batch_delete_async_internal_static(store, &table, keys).await
            })
        })
    }

    /// Scan a range of keys (blocking - use from sync context or spawn_blocking)
    pub fn scan(&self, table: &str, start_key: impl Into<Bytes>, end_key: Option<impl Into<Bytes>>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let store = self.store.clone();
        let table = table.to_string();
        let start_key = start_key.into();
        let end_key = end_key.map(|k| k.into());

        tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                Self::scan_async_internal_static(store, &table, start_key, end_key, limit).await
            })
        })
    }

    /// Check if a key exists (blocking - use from sync context or spawn_blocking)
    pub fn exists(&self, table: &str, key: impl Into<Bytes>) -> Result<bool> {
        Ok(self.get(table, key)?.is_some())
    }

    // ========== Direct Async Operations (for use in async contexts) ==========

    /// Get a value by key (async)
    pub async fn get_async(&self, table: &str, key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        self.get_async_internal(table, key).await
    }

    /// Get a value by key (async) - alternative name for stress test
    pub async fn async_get(&self, table: &str, key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        self.get_async_internal(table, key).await
    }

    /// Set a key-value pair (async)
    pub async fn set_async(&self, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.set_async_internal(table, key, value).await
    }

    /// Set a key-value pair (async) - alternative name for stress test
    pub async fn async_set(&self, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.set_async_internal(table, key, value).await
    }

    /// Delete a key (async)
    pub async fn delete_async(&self, table: &str, key: impl Into<Bytes>) -> Result<()> {
        self.delete_async_internal(table, key).await
    }

    /// Delete a key (async) - alternative name for stress test
    pub async fn async_delete(&self, table: &str, key: impl Into<Bytes>) -> Result<()> {
        self.delete_async_internal(table, key).await
    }

    /// Get multiple values by keys (async)
    pub async fn batch_get_async(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<Vec<Option<Bytes>>> {
        self.batch_get_async_internal(table, keys).await
    }

    /// Get multiple values by keys (async) - alternative name for stress test
    pub async fn async_batch_get(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<Vec<Option<Bytes>>> {
        self.batch_get_async_internal(table, keys).await
    }

    /// Set multiple key-value pairs (async)
    pub async fn batch_set_async(&self, table: &str, kvs: Vec<(impl Into<Bytes>, impl Into<Bytes>)>) -> Result<()> {
        self.batch_set_async_internal(table, kvs).await
    }

    /// Set multiple key-value pairs (async) - alternative name for stress test
    pub async fn async_batch_set(&self, table: &str, kvs: Vec<(impl Into<Bytes>, impl Into<Bytes>)>) -> Result<()> {
        self.batch_set_async_internal(table, kvs).await
    }

    /// Delete multiple keys (async)
    pub async fn batch_delete_async(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<()> {
        self.batch_delete_async_internal(table, keys).await
    }

    /// Scan a range of keys (async)
    pub async fn scan_async(&self, table: &str, start_key: impl Into<Bytes>, end_key: Option<impl Into<Bytes>>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_async_internal(table, start_key, end_key, limit).await
    }

    /// Simple scan from a start key with limit (async) - for stress test
    pub async fn async_scan(&self, table: &str, start_key: impl Into<Bytes>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_async_internal(table, start_key, None::<Bytes>, Some(limit)).await
    }

    /// Check if a key exists (async)
    pub async fn exists_async(&self, table: &str, key: impl Into<Bytes>) -> Result<bool> {
        Ok(self.get_async(table, key).await?.is_some())
    }

    // ========== Promise-style Asynchronous Operations (Return AsyncResult) ==========

    /// Get a value by key (async, returns promise)
    pub fn promise_get(&self, table: &str, key: impl Into<Bytes>) -> AsyncResult<Option<Bytes>> {
        let (tx, rx) = oneshot::channel();
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();

        self.runtime.spawn(async move {
            let result = Self::get_async_internal_static(store, &table, key).await;
            let _ = tx.send(result);
        });

        AsyncResult { receiver: rx }
    }

    /// Set a key-value pair (async, returns promise)
    pub fn promise_set(&self, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> AsyncResult<()> {
        let (tx, rx) = oneshot::channel();
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();
        let value = value.into();

        self.runtime.spawn(async move {
            let result = Self::set_async_internal_static(store, &table, key, value).await;
            let _ = tx.send(result);
        });

        AsyncResult { receiver: rx }
    }

    /// Delete a key (async, returns promise)
    pub fn promise_delete(&self, table: &str, key: impl Into<Bytes>) -> AsyncResult<()> {
        let (tx, rx) = oneshot::channel();
        let store = self.store.clone();
        let table = table.to_string();
        let key = key.into();

        self.runtime.spawn(async move {
            let result = Self::delete_async_internal_static(store, &table, key).await;
            let _ = tx.send(result);
        });

        AsyncResult { receiver: rx }
    }

    /// Get multiple values (async, returns promise)
    pub fn promise_batch_get(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> AsyncResult<Vec<Option<Bytes>>> {
        let (tx, rx) = oneshot::channel();
        let store = self.store.clone();
        let table = table.to_string();
        let keys: Vec<Bytes> = keys.into_iter().map(|k| k.into()).collect();

        self.runtime.spawn(async move {
            let result = Self::batch_get_async_internal_static(store, &table, keys).await;
            let _ = tx.send(result);
        });

        AsyncResult { receiver: rx }
    }

    /// Set multiple key-value pairs (async, returns promise)
    pub fn promise_batch_set(&self, table: &str, kvs: Vec<(impl Into<Bytes>, impl Into<Bytes>)>) -> AsyncResult<()> {
        let (tx, rx) = oneshot::channel();
        let store = self.store.clone();
        let table = table.to_string();
        let kvs: Vec<(Bytes, Bytes)> = kvs.into_iter().map(|(k, v)| (k.into(), v.into())).collect();

        self.runtime.spawn(async move {
            let result = Self::batch_set_async_internal_static(store, &table, kvs).await;
            let _ = tx.send(result);
        });

        AsyncResult { receiver: rx }
    }

    // ========== Internal async implementations ==========

    async fn get_async_internal(&self, table: &str, key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        Self::get_async_internal_static(self.store.clone(), table, key).await
    }

    async fn get_async_internal_static(store: Arc<EloqStore>, table: &str, key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        let req = ReadRequest {
            table_id: TableIdent::new(table, 0),
            key: key.into(),
            timeout: None,
        };

        let resp = store.read(req).await?;
        Ok(resp.value)
    }

    async fn set_async_internal(&self, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        Self::set_async_internal_static(self.store.clone(), table, key, value).await
    }

    async fn set_async_internal_static(store: Arc<EloqStore>, table: &str, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let req = BatchWriteRequest {
            table_id: TableIdent::new(table, 0),
            entries: vec![WriteEntry {
                key: key.into(),
                value: value.into(),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            }],
            sync: true,
            timeout: None,
        };

        store.batch_write(req).await?;
        Ok(())
    }

    async fn delete_async_internal(&self, table: &str, key: impl Into<Bytes>) -> Result<()> {
        Self::delete_async_internal_static(self.store.clone(), table, key).await
    }

    async fn delete_async_internal_static(store: Arc<EloqStore>, table: &str, key: impl Into<Bytes>) -> Result<()> {
        let req = BatchWriteRequest {
            table_id: TableIdent::new(table, 0),
            entries: vec![WriteEntry {
                key: key.into(),
                value: Bytes::new(),
                op: WriteOp::Delete,
                timestamp: 0,
                expire_ts: None,
            }],
            sync: true,
            timeout: None,
        };

        store.batch_write(req).await?;
        Ok(())
    }

    async fn batch_get_async_internal(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<Vec<Option<Bytes>>> {
        let keys: Vec<Bytes> = keys.into_iter().map(|k| k.into()).collect();
        Self::batch_get_async_internal_static(self.store.clone(), table, keys).await
    }

    async fn batch_get_async_internal_static(store: Arc<EloqStore>, table: &str, keys: Vec<Bytes>) -> Result<Vec<Option<Bytes>>> {
        let mut results = Vec::new();

        // For now, issue individual reads (could optimize with parallel futures)
        for key in keys {
            let req = ReadRequest {
                table_id: TableIdent::new(table, 0),
                key,
                timeout: None,
            };

            let resp = store.read(req).await?;
            results.push(resp.value);
        }

        Ok(results)
    }

    async fn batch_set_async_internal(&self, table: &str, kvs: Vec<(impl Into<Bytes>, impl Into<Bytes>)>) -> Result<()> {
        let kvs: Vec<(Bytes, Bytes)> = kvs.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        Self::batch_set_async_internal_static(self.store.clone(), table, kvs).await
    }

    async fn batch_set_async_internal_static(store: Arc<EloqStore>, table: &str, kvs: Vec<(Bytes, Bytes)>) -> Result<()> {
        let entries: Vec<WriteEntry> = kvs.into_iter().map(|(key, value)| {
            WriteEntry {
                key,
                value,
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            }
        }).collect();

        let req = BatchWriteRequest {
            table_id: TableIdent::new(table, 0),
            entries,
            sync: true,
            timeout: None,
        };

        store.batch_write(req).await?;
        Ok(())
    }

    async fn batch_delete_async_internal(&self, table: &str, keys: Vec<impl Into<Bytes>>) -> Result<()> {
        let keys: Vec<Bytes> = keys.into_iter().map(|k| k.into()).collect();
        Self::batch_delete_async_internal_static(self.store.clone(), table, keys).await
    }

    async fn batch_delete_async_internal_static(store: Arc<EloqStore>, table: &str, keys: Vec<Bytes>) -> Result<()> {
        let entries: Vec<WriteEntry> = keys.into_iter().map(|key| {
            WriteEntry {
                key,
                value: Bytes::new(),
                op: WriteOp::Delete,
                timestamp: 0,
                expire_ts: None,
            }
        }).collect();

        let req = BatchWriteRequest {
            table_id: TableIdent::new(table, 0),
            entries,
            sync: true,
            timeout: None,
        };

        store.batch_write(req).await?;
        Ok(())
    }

    async fn scan_async_internal(&self, table: &str, start_key: impl Into<Bytes>, end_key: Option<impl Into<Bytes>>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let start_key = start_key.into();
        let end_key = end_key.map(|k| k.into());
        Self::scan_async_internal_static(self.store.clone(), table, start_key, end_key, limit).await
    }

    async fn scan_async_internal_static(store: Arc<EloqStore>, table: &str, start_key: Bytes, end_key: Option<Bytes>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let req = ScanRequest {
            table_id: TableIdent::new(table, 0),
            start_key,
            end_key,
            limit,
            reverse: false,
            timeout: None,
        };

        let resp = store.scan(req).await?;
        Ok(resp.entries)
    }
}

// Convenience methods for string conversions
impl Store {
    /// Get a value using string key
    pub fn get_str(&self, table: &str, key: &str) -> Result<Option<String>> {
        let key_bytes = Bytes::from(key.to_string());
        self.get(table, key_bytes)?
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .map_err(|e| Error::InvalidState(format!("Invalid UTF-8: {}", e)))
    }

    /// Set a string key-value pair
    pub fn set_str(&self, table: &str, key: &str, value: &str) -> Result<()> {
        let key_bytes = Bytes::from(key.to_string());
        let value_bytes = Bytes::from(value.to_string());
        self.set(table, key_bytes, value_bytes)
    }
}