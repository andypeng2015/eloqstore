#pragma once

#include <cstdint>
#include <string>

#include "comparator.h"

namespace kvstore
{
constexpr uint8_t max_overflow_pointers = 128;
constexpr uint16_t max_key_size = 2048;

struct KvOptions
{
    /**
     * @brief Key-Value database storage path.
     * In-memory storage will be used if path is empty.
     */
    std::string db_path;
    /**
     * @brief Amount of threads.
     */
    uint16_t num_threads = 1;

    const Comparator *comparator_ = Comparator::DefaultComparator();
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint16_t index_page_read_queue = 1024;
    uint32_t init_page_count = 1 << 15;

    /**
     * @brief Max amount of cached index pages per thread.
     */
    uint32_t index_buffer_pool_size = UINT32_MAX;
    /**
     * @brief Limit manifest file size.
     */
    uint64_t manifest_limit = 8 << 20;  // 8MB
    /**
     * @brief Max amount of opened files per thread.
     */
    uint32_t fd_limit = 1024;
    /**
     * @brief Size of io-uring submission queue per thread.
     */
    uint32_t io_queue_size = 4096;
    /**
     * @brief Max amount of inflight write IO per thread.
     */
    uint32_t max_inflight_write = 4096;
    /**
     * @brief Size of io-uring selected buffer ring.
     * It must be a power-of 2, and can be up to 32768.
     */
    uint16_t buf_ring_size = 1 << 10;
    /**
     * @brief Size of coroutine stack.
     * According to the latest test results, at least 16KB is required.
     */
    uint32_t coroutine_stack_size = 16 * 1024;

    /*
     * The following options will be persisted. User cannot change them after
     * setting for the first time.
     */

    /**
     * @brief Size of B+Tree index/data node (page).
     * Ensure that it is aligned to the system's page size.
     */
    uint16_t data_page_size = 1 << 12;  // 4KB
    /**
     * @brief Amount of pages per data file (1 << num_file_pages_shift).
     */
    uint8_t num_file_pages_shift = 11;  // 2048
    /**
     * @brief Amount of pointers stored in overflow page.
     * The maximum value is 128.
     */
    uint8_t overflow_pointers = 16;
};

}  // namespace kvstore