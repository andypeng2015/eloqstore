#pragma once

#include <unistd.h>

#include <cstdint>
#include <string>

#include "comparator.h"

namespace kvstore
{
struct KvOptions
{
    uint16_t num_workers = 1;
    const Comparator *comparator_ = Comparator::DefaultComparator();

    uint16_t data_page_size = 1 << 12;  // 4KB
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint16_t index_page_read_queue = 1024;
    uint32_t index_buffer_pool_size = UINT32_MAX;

    uint8_t io_read_buffer_size = 128;
    uint8_t io_write_buffer_size = 128;
    uint8_t task_write_buffer = 128;

    uint16_t file_page_count = 2048;
    uint32_t init_page_count = 1 << 15;

    std::string data_path;
    uint8_t data_file_pages = 11;       // 1 << data_file_pages
    uint64_t manifest_limit = 8 << 20;  // 8MB
    uint32_t fd_limit = 1024;
    uint32_t io_queue_size = 4096;
    uint16_t buf_ring_size = 1 << 10;
    uint32_t coroutine_stack_size = 8 * 1024;
};

inline static size_t page_align = sysconf(_SC_PAGESIZE);

}  // namespace kvstore