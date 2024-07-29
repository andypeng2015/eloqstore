#pragma once

#include <cstdint>

#include "comparator.h"

namespace kvstore
{
struct KvOptions
{
    const Comparator *comparator_ = Comparator::DefaultComparator();

    uint16_t data_page_size = 1 << 7;   // 4KB
    uint16_t index_page_size = 1 << 7;  // 4KB
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint16_t index_page_read_queue = 1024;
    uint32_t index_buffer_pool_size = UINT32_MAX;

    uint8_t io_read_buffer_size = 128;
    uint8_t io_write_buffer_size = 128;
    uint8_t task_write_buffer = 128;

    uint16_t file_page_count = 2048;
    uint32_t init_page_count = 1 << 15;
};
}  // namespace kvstore