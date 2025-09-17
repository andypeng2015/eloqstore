//! Test example demonstrating cache eviction

use eloqstore::page::{PageCache, PageCacheConfig, EvictionPolicy, DataPage, DataPageBuilder};
use eloqstore::types::PageId;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;

#[tokio::main]
async fn main() {
    println!("=== Cache Eviction Test ===\n");

    // Create a small cache for testing (1MB max, 800KB target)
    let mut config = PageCacheConfig::default();
    config.max_size = 1024 * 1024; // 1MB
    config.target_size = 800 * 1024; // 800KB
    config.high_water_mark = 0.9; // Start eviction at 90%
    config.low_water_mark = 0.7; // Stop eviction at 70%
    config.ttl = Duration::from_secs(60);
    config.eviction_policy = EvictionPolicy::LRU;
    config.eviction_interval = Duration::from_secs(1); // Check every second

    let cache = Arc::new(PageCache::new(config));

    // Start background eviction
    println!("Starting background eviction task...");
    cache.clone().start_background_eviction().await;

    // Test 1: Fill cache with pages
    println!("\n1. Filling cache with pages:");
    let page_size = 4096; // 4KB pages
    let num_pages = 300; // 300 * 4KB = 1.2MB (exceeds cache size)

    for i in 0..num_pages {
        // Create a data page
        let page_id = i as PageId;
        let mut builder = DataPageBuilder::new(page_size);

        // Add some data
        let key = format!("key_{:04}", i);
        let value = vec![b'X'; 1000]; // 1KB value
        builder.add(key.as_bytes(), &value, i as u64, None, false);

        let page = Arc::new(builder.finish(page_id));

        // Insert into cache
        cache.insert_page(page).await;

        if (i + 1) % 50 == 0 {
            let stats = cache.stats().await;
            println!("   After {} pages: size={} bytes, cached={}, evicted={}",
                i + 1, stats.current_size, stats.page_count, stats.evictions);
        }
    }

    // Test 2: Access pattern to test LRU
    println!("\n2. Testing LRU eviction:");

    // Access some early pages to make them "hot"
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        if let Some(_page) = cache.get_by_slice(key.as_bytes()).await {
            println!("   Page {} still in cache (accessed)", i);
        } else {
            println!("   Page {} was evicted", i);
        }
    }

    // Check middle pages (should be evicted)
    for i in 100..110 {
        let key = format!("key_{:04}", i);
        if cache.get_by_slice(key.as_bytes()).await.is_some() {
            println!("   Page {} still in cache", i);
        } else {
            println!("   Page {} was evicted (expected)", i);
        }
    }

    // Test 3: Pin pages to prevent eviction
    println!("\n3. Testing page pinning:");

    // Pin some recent pages
    for i in 290..295 {
        cache.pin(i).await;
        println!("   Pinned page {}", i);
    }

    // Add more pages to trigger eviction
    println!("\n   Adding more pages to trigger eviction...");
    for i in 300..350 {
        let page_id = i as PageId;
        let mut builder = DataPageBuilder::new(page_size);
        let key = format!("key_{:04}", i);
        let value = vec![b'Y'; 1000];
        builder.add(key.as_bytes(), &value, i as u64, None, false);
        let page = Arc::new(builder.finish(page_id));
        cache.insert_page(page).await;
    }

    // Check if pinned pages are still there
    println!("\n   Checking pinned pages:");
    for i in 290..295 {
        let key = format!("key_{:04}", i);
        if cache.get_by_slice(key.as_bytes()).await.is_some() {
            println!("   Pinned page {} still in cache ✓", i);
        } else {
            println!("   Pinned page {} was evicted ✗ (should not happen)", i);
        }
        cache.unpin(i).await;
    }

    // Test 4: TTL expiration
    println!("\n4. Testing TTL expiration:");

    // Create cache with short TTL
    let mut short_ttl_config = PageCacheConfig::default();
    short_ttl_config.max_size = 1024 * 1024;
    short_ttl_config.ttl = Duration::from_secs(2);
    let ttl_cache = Arc::new(PageCache::new(short_ttl_config));

    // Add a page
    let page_id = 9999 as PageId;
    let mut builder = DataPageBuilder::new(page_size);
    builder.add(b"ttl_test_key", b"ttl_test_value", 9999, None, false);
    let page = Arc::new(builder.finish(page_id));
    ttl_cache.insert_page(page).await;

    // Check immediately
    if ttl_cache.get_by_slice(b"ttl_test_key").await.is_some() {
        println!("   Page accessible immediately ✓");
    }

    // Wait for TTL to expire
    println!("   Waiting 3 seconds for TTL to expire...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check after expiration
    if ttl_cache.get_by_slice(b"ttl_test_key").await.is_none() {
        println!("   Page expired after TTL ✓");
    } else {
        println!("   Page still accessible after TTL ✗");
    }

    // Final statistics
    println!("\n5. Final Cache Statistics:");
    let final_stats = cache.stats().await;
    println!("   Total insertions: {}", final_stats.insertions);
    println!("   Total evictions: {}", final_stats.evictions);
    println!("   Total hits: {}", final_stats.hits);
    println!("   Total misses: {}", final_stats.misses);
    println!("   Current size: {} bytes", final_stats.current_size);
    println!("   Cached pages: {}", final_stats.page_count);
    println!("   Pinned pages: {}", final_stats.pinned_count);

    let efficiency = if final_stats.hits + final_stats.misses > 0 {
        (final_stats.hits as f64 / (final_stats.hits + final_stats.misses) as f64) * 100.0
    } else {
        0.0
    };
    println!("   Cache hit rate: {:.1}%", efficiency);

    // Stop background eviction
    cache.stop_background_eviction().await;
    println!("\n✓ Test completed successfully!");
}