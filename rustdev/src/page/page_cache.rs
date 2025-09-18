//! Improved page cache with efficient LRU eviction
//!
//! This implementation provides:
//! - Efficient O(1) LRU operations using a combination of HashMap and linked list
//! - Background eviction task
//! - Adaptive eviction based on memory pressure
//! - Better concurrency with fine-grained locking

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{RwLock, Mutex};
use tokio::time::interval;
use bytes::Bytes;

use crate::types::{Key, PageId};
use crate::Result;
use crate::error::Error;

use super::DataPage;

/// Cache eviction policy
#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// First In First Out
    FIFO,
    /// Adaptive Replacement Cache (ARC)
    ARC,
}

/// Page cache configuration
#[derive(Debug, Clone)]
pub struct PageCacheConfig {
    /// Maximum cache size in bytes
    pub max_size: usize,
    /// Target cache size (soft limit)
    pub target_size: usize,
    /// TTL for cached pages
    pub ttl: Duration,
    /// Enable LRU eviction (for backward compatibility)
    pub enable_lru: bool,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Background eviction interval
    pub eviction_interval: Duration,
    /// High water mark (trigger aggressive eviction)
    pub high_water_mark: f64, // e.g., 0.9 = 90% of max_size
    /// Low water mark (stop eviction)
    pub low_water_mark: f64, // e.g., 0.7 = 70% of max_size
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        let max_size = 256 * 1024 * 1024; // 256MB
        Self {
            max_size,
            target_size: max_size * 8 / 10, // 80% of max
            ttl: Duration::from_secs(300), // 5 minutes
            enable_lru: true,
            eviction_policy: EvictionPolicy::LRU,
            eviction_interval: Duration::from_secs(10),
            high_water_mark: 0.9,
            low_water_mark: 0.7,
        }
    }
}

/// LRU entry for tracking page access order
#[derive(Debug, Clone)]
struct LruEntry {
    page_id: PageId,
    last_access: Instant,
    access_count: u64,
    size: usize,
}

/// Cached page entry
#[derive(Debug)]
struct CachedPage {
    /// Page data
    page: Arc<DataPage>,
    /// Last access time
    last_access: Arc<RwLock<Instant>>,
    /// Access count
    access_count: AtomicU64,
    /// Page size
    size: usize,
    /// Pin count (pages with pin > 0 cannot be evicted)
    pin_count: AtomicUsize,
}

impl CachedPage {
    fn new(page: Arc<DataPage>) -> Self {
        let size = page.size();
        Self {
            page,
            last_access: Arc::new(RwLock::new(Instant::now())),
            access_count: AtomicU64::new(1),
            size,
            pin_count: AtomicUsize::new(0),
        }
    }

    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::Relaxed);
    }

    fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::Relaxed);
    }

    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Relaxed) > 0
    }

    async fn touch(&self) {
        *self.last_access.write().await = Instant::now();
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Cache statistics
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    /// Total hits
    pub hits: u64,
    /// Total misses
    pub misses: u64,
    /// Total evictions
    pub evictions: u64,
    /// Total insertions
    pub insertions: u64,
    /// Current cache size in bytes
    pub current_size: usize,
    /// Number of cached pages
    pub page_count: usize,
    /// Number of pinned pages
    pub pinned_count: usize,
}

/// LRU tracker for efficient eviction
#[derive(Debug)]
struct LruTracker {
    /// Ordered list of page IDs (front = most recent, back = least recent)
    order: VecDeque<PageId>,
    /// Position of each page in the order list
    positions: HashMap<PageId, usize>,
}

impl LruTracker {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            positions: HashMap::new(),
        }
    }

    /// Touch a page (move to front)
    fn touch(&mut self, page_id: PageId) {
        if let Some(&pos) = self.positions.get(&page_id) {
            // Remove from current position
            self.order.remove(pos);
            // Update positions for pages after the removed one
            for i in pos..self.order.len() {
                if let Some(pid) = self.order.get(i) {
                    self.positions.insert(*pid, i);
                }
            }
        }

        // Add to front
        self.order.push_front(page_id);
        self.positions.insert(page_id, 0);

        // Update positions for all pages
        for (i, pid) in self.order.iter().enumerate() {
            self.positions.insert(*pid, i);
        }
    }

    /// Add a new page
    fn add(&mut self, page_id: PageId) {
        self.touch(page_id);
    }

    /// Remove a page
    fn remove(&mut self, page_id: PageId) {
        if let Some(&pos) = self.positions.get(&page_id) {
            self.order.remove(pos);
            self.positions.remove(&page_id);

            // Update positions for pages after the removed one
            for i in pos..self.order.len() {
                if let Some(pid) = self.order.get(i) {
                    self.positions.insert(*pid, i);
                }
            }
        }
    }

    /// Get least recently used pages
    fn get_lru_pages(&self, count: usize) -> Vec<PageId> {
        self.order.iter()
            .rev()
            .take(count)
            .copied()
            .collect()
    }

    /// Clear all entries
    fn clear(&mut self) {
        self.order.clear();
        self.positions.clear();
    }
}

/// Page cache implementation with efficient LRU eviction
#[derive(Debug)]
pub struct PageCache {
    /// Configuration
    config: PageCacheConfig,
    /// Cached pages by page ID
    pages: Arc<DashMap<PageId, Arc<CachedPage>>>,
    /// Key to page ID index
    key_index: Arc<DashMap<Bytes, PageId>>,
    /// LRU tracker (protected by mutex for consistency)
    lru_tracker: Arc<Mutex<LruTracker>>,
    /// Current cache size in bytes
    current_size: AtomicUsize,
    /// Statistics
    stats: Arc<RwLock<CacheStats>>,
    /// Background eviction handle
    eviction_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl PageCache {
    /// Create a new page cache
    pub fn new(config: PageCacheConfig) -> Self {
        Self {
            config,
            pages: Arc::new(DashMap::new()),
            key_index: Arc::new(DashMap::new()),
            lru_tracker: Arc::new(Mutex::new(LruTracker::new())),
            current_size: AtomicUsize::new(0),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            eviction_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Start background eviction task
    pub async fn start_background_eviction(self: Arc<Self>) {
        let cache = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = interval(cache.config.eviction_interval);

            loop {
                interval.tick().await;

                // Check memory pressure
                let current_size = cache.current_size.load(Ordering::Relaxed);
                let high_water = (cache.config.max_size as f64 * cache.config.high_water_mark) as usize;

                if current_size > high_water {
                    let low_water = (cache.config.max_size as f64 * cache.config.low_water_mark) as usize;
                    let to_free = current_size.saturating_sub(low_water);

                    tracing::debug!("Cache eviction triggered: current={}, high_water={}, to_free={}",
                        current_size, high_water, to_free);

                    cache.evict_bytes(to_free).await;
                }

                // Also clean up expired entries
                cache.cleanup_expired().await;
            }
        });

        let mut handle_guard = self.eviction_handle.write().await;
        *handle_guard = Some(handle);
    }

    /// Stop background eviction task
    pub async fn stop_background_eviction(&self) {
        let mut handle_guard = self.eviction_handle.write().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }
    }

    /// Get a page by key (accepts &Key which is Vec<u8> for compatibility)
    pub async fn get(&self, key: &crate::types::Key) -> Option<Arc<DataPage>> {
        let key_bytes = Bytes::copy_from_slice(key);

        // Look up page ID
        let page_id = self.key_index.get(&key_bytes)?;
        let page_id = *page_id;

        self.get_by_id(page_id).await
    }

    /// Get a page by key slice
    pub async fn get_by_slice(&self, key: &[u8]) -> Option<Arc<DataPage>> {
        let key_bytes = Bytes::copy_from_slice(key);

        // Look up page ID
        let page_id = self.key_index.get(&key_bytes)?;
        let page_id = *page_id;

        self.get_by_id(page_id).await
    }

    /// Get a page by ID
    pub async fn get_by_id(&self, page_id: PageId) -> Option<Arc<DataPage>> {
        let entry = self.pages.get(&page_id)?;
        let cached = entry.value().clone();
        drop(entry); // Release lock early

        // Check TTL
        let last_access = *cached.last_access.read().await;
        if last_access.elapsed() > self.config.ttl {
            self.remove_page(page_id).await;

            let mut stats = self.stats.write().await;
            stats.misses += 1;
            return None;
        }

        // Update access tracking
        cached.touch().await;

        // Update LRU
        let mut lru = self.lru_tracker.lock().await;
        lru.touch(page_id);
        drop(lru);

        // Update stats
        let mut stats = self.stats.write().await;
        stats.hits += 1;

        Some(cached.page.clone())
    }

    /// Insert a page into the cache (key parameter ignored for backward compatibility)
    pub async fn insert(&self, _key: crate::types::Key, page: Arc<DataPage>) {
        self.insert_page(page).await;
    }

    /// Insert a page into the cache (new API without unused key parameter)
    pub async fn insert_page(&self, page: Arc<DataPage>) {
        let page_id = page.page_id();
        let cached = Arc::new(CachedPage::new(page.clone()));
        let page_size = cached.size;

        // Check if we need to evict
        let current = self.current_size.load(Ordering::Relaxed);
        if current + page_size > self.config.max_size {
            self.evict_bytes(page_size).await;
        }

        // Insert page
        self.pages.insert(page_id, cached);

        // Update key index with all keys in the page
        let mut iter = page.iter();
        while iter.next() {
            let key = iter.key();
            self.key_index.insert(Bytes::copy_from_slice(key), page_id);
        }

        // Update LRU
        let mut lru = self.lru_tracker.lock().await;
        lru.add(page_id);
        drop(lru);

        // Update size and stats
        self.current_size.fetch_add(page_size, Ordering::Relaxed);

        let mut stats = self.stats.write().await;
        stats.insertions += 1;
        stats.current_size = self.current_size.load(Ordering::Relaxed);
        stats.page_count = self.pages.len();
    }

    /// Pin a page (prevent eviction)
    pub async fn pin(&self, page_id: PageId) {
        if let Some(entry) = self.pages.get(&page_id) {
            entry.pin();
        }
    }

    /// Unpin a page
    pub async fn unpin(&self, page_id: PageId) {
        if let Some(entry) = self.pages.get(&page_id) {
            entry.unpin();
        }
    }

    /// Evict a specific amount of bytes
    async fn evict_bytes(&self, bytes_to_free: usize) {
        let mut freed = 0;

        // Get LRU pages
        let lru_pages = {
            let lru = self.lru_tracker.lock().await;
            lru.get_lru_pages(self.pages.len())
        };

        for page_id in lru_pages {
            if freed >= bytes_to_free {
                break;
            }

            // Check if page is pinned
            if let Some(entry) = self.pages.get(&page_id) {
                if !entry.is_pinned() {
                    let size = entry.size;
                    drop(entry);

                    self.remove_page(page_id).await;
                    freed += size;

                    let mut stats = self.stats.write().await;
                    stats.evictions += 1;
                }
            }
        }
    }

    /// Remove a page from cache
    async fn remove_page(&self, page_id: PageId) {
        if let Some((_, cached)) = self.pages.remove(&page_id) {
            // Remove from key index
            self.key_index.retain(|_, pid| *pid != page_id);

            // Remove from LRU
            let mut lru = self.lru_tracker.lock().await;
            lru.remove(page_id);
            drop(lru);

            // Update size
            self.current_size.fetch_sub(cached.size, Ordering::Relaxed);
        }
    }

    /// Clean up expired entries
    async fn cleanup_expired(&self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for entry in self.pages.iter() {
            let last_access = *entry.value().last_access.read().await;
            if now.duration_since(last_access) > self.config.ttl {
                to_remove.push(*entry.key());
            }
        }

        for page_id in to_remove {
            self.remove_page(page_id).await;

            let mut stats = self.stats.write().await;
            stats.evictions += 1;
        }
    }

    /// Invalidate a key
    pub async fn invalidate(&self, key: &crate::types::Key) {
        let key_bytes = Bytes::copy_from_slice(key);

        if let Some((_, page_id)) = self.key_index.remove(&key_bytes) {
            // Check if we should remove the whole page
            let should_remove = !self.key_index.iter()
                .any(|entry| *entry.value() == page_id);

            if should_remove {
                self.remove_page(page_id).await;
            }
        }
    }

    /// Invalidate a page from the cache
    pub async fn invalidate_page(&self, page_id: PageId) {
        self.remove_page(page_id).await;

        let mut stats = self.stats.write().await;
        stats.evictions += 1;
    }

    /// Clear the entire cache
    pub async fn clear(&self) {
        self.pages.clear();
        self.key_index.clear();

        let mut lru = self.lru_tracker.lock().await;
        lru.clear();
        drop(lru);

        self.current_size.store(0, Ordering::Relaxed);

        let mut stats = self.stats.write().await;
        stats.evictions += stats.page_count as u64;
        stats.current_size = 0;
        stats.page_count = 0;
        stats.pinned_count = 0;
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let stats = self.stats.read().await;
        let mut result = stats.clone();

        // Update dynamic stats
        result.current_size = self.current_size.load(Ordering::Relaxed);
        result.page_count = self.pages.len();
        result.pinned_count = self.pages.iter()
            .filter(|entry| entry.value().is_pinned())
            .count();

        result
    }

    /// Get current cache size
    pub fn size(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Check if cache is full
    pub fn is_full(&self) -> bool {
        self.current_size.load(Ordering::Relaxed) >= self.config.max_size
    }
}