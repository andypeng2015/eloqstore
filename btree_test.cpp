#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <ostream>
#include <string_view>
#include <vector>

#include "async_io_manager.h"
#include "coding.h"
#include "global_variables.h"
#include "index_page_builder.h"
#include "index_page_manager.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "read_task.h"
#include "storage_manager.h"
#include "write_task.h"

int32_t Floor(const std::vector<int> arr, int val)
{
    if (arr.size() == 0)
    {
        return -1;
    }

    size_t cnt = arr.size();
    int32_t right = arr.size() - 1;

    while (cnt > 0)
    {
        size_t step = cnt >> 1;
        size_t mid = right - step;

        int pivot = arr[mid];
        if (pivot > val)
        {
            right = mid - 1;
            cnt -= step + 1;
        }
        else
        {
            cnt = step;
        }
    }

    return right;
}

std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = kvstore::ToBigEndian(key);
    kvstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = kvstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}

void BuildIter(const kvstore::KvOptions *opt)
{
    kvstore::IndexPageBuilder index_p_builder(opt);

    uint64_t val = 128;
    bool is_full = false;
    size_t max_cnt = UINT64_MAX, idx = 0;
    do
    {
        uint64_t big_endian = kvstore::ToBigEndian(val * 2);
        char key[sizeof(uint64_t)];
        kvstore::EncodeFixed64(key, big_endian);

        std::string key_str{key, sizeof(uint64_t)};
        uint64_t pointer = UINT16_MAX - idx;

        is_full = !index_p_builder.Add(key_str, pointer, false);

        ++val;
        ++idx;
    } while (!is_full && idx < max_cnt);

    std::string_view blob_view = index_p_builder.Finish();
    assert(blob_view.size() == opt->index_page_size);

    kvstore::MemIndexPage idx_page;
    memcpy(idx_page.PagePtr(), blob_view.data(), blob_view.size());

    std::cout << "Page size: " << idx_page.ContentLength() << std::endl;

    kvstore::IndexPageIter iter{&idx_page, opt->comparator_};

    while (iter.HasNext())
    {
        iter.Next();
        std::string_view key = iter.Key();
        uint32_t pt = iter.PageId();

        if (key.size() == 0)
        {
            std::cout << "neg, val: " << pt << std::endl;
            continue;
        }

        uint64_t big_endian = kvstore::DecodeFixed64(key.data());
        uint64_t original = __builtin_bswap64(big_endian);

        std::cout << "key: " << original << ", val: " << pt << std::endl;
    }

    iter.Reset();
    char key_buf[sizeof(uint64_t)];

    uint64_t k1 = 267;
    std::string_view key = ConvertIntKey(key_buf, k1);
    iter.Seek(key);
    std::string_view idx_key = iter.Key();
    uint32_t idx_page_id = iter.PageId();
    if (idx_key.empty())
    {
        std::cout << "Seek " << k1
                  << ", idx key: null, idx ptr: " << idx_page_id << std::endl;
    }
    else
    {
        std::cout << "Seek " << k1 << ", idx key: " << ConvertIntKey(idx_key)
                  << ", idx ptr: " << idx_page_id << std::endl;
    }
}

void InitData()
{
    std::string tbl_name{"t1"};
    uint32_t partition_id = 0;

    kvstore::WriteTask write_task{tbl_name, partition_id, &kvstore::kv_options};
    kvstore::thd_task = &write_task;
    write_task.Reset(kvstore::idx_manager.get());
    size_t n = 1000;
    uint64_t base_ts = 2;

    for (size_t idx = 0; idx < n; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           base_ts,
                           kvstore::WriteOp::Upsert);
    }

    write_task.Apply();

    kvstore::TableIdent tbl_ident{tbl_name, partition_id};
    kvstore::ReadTask read_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &read_task;
    for (size_t idx = 0; idx < n; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string_view search_key{buf, sizeof(uint64_t)};
        std::string_view val;
        uint64_t ts;
        kvstore::KvError err = read_task.Read(tbl_ident, search_key, val, ts);
        assert(err == kvstore::KvError::NoError);
        std::string int_str = std::to_string(idx);
        assert(val == int_str);
        assert(ts == base_ts);
    }
}

void Upsert()
{
    InitData();

    std::string tbl_name{"t1"};
    uint32_t partition_id = 0;

    kvstore::WriteTask write_task{tbl_name, partition_id, &kvstore::kv_options};
    kvstore::thd_task = &write_task;
    write_task.Reset(kvstore::idx_manager.get());
    size_t n = 1000;
    uint64_t base_ts = 2;

    kvstore::thd_task = &write_task;

    n = 50;
    for (size_t idx = 100; idx < n + 100; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           1,
                           kvstore::WriteOp::Upsert);
    }

    for (size_t idx = 800; idx < n + 800; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           1,
                           kvstore::WriteOp::Upsert);
    }

    write_task.Apply();

    kvstore::TableIdent tbl_ident{tbl_name, partition_id};
    kvstore::ReadTask read_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &read_task;

    n = 1000;
    for (size_t idx = 0; idx < n; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string_view search_key{buf, sizeof(uint64_t)};
        std::string_view val;
        uint64_t ts;
        kvstore::KvError err = read_task.Read(tbl_ident, search_key, val, ts);
        assert(err == kvstore::KvError::NoError);
        std::string int_str = std::to_string(idx);
        assert(val == int_str);
        assert(ts == base_ts);
    }

    write_task.Reset(kvstore::idx_manager.get());
    kvstore::thd_task = &write_task;

    n = 100;
    for (size_t idx = 100; idx < n + 100; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx * 2);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           3,
                           kvstore::WriteOp::Upsert);
    }

    write_task.Apply();

    n = 1000;
    for (size_t idx = 0; idx < n; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string_view search_key{buf, sizeof(uint64_t)};
        std::string_view val;
        uint64_t ts;
        kvstore::KvError err = read_task.Read(tbl_ident, search_key, val, ts);
        assert(err == kvstore::KvError::NoError);
        size_t p_idx = idx;
        if (idx >= 100 && idx < 200)
        {
            p_idx = idx * 2;
        }
        std::string int_str = std::to_string(p_idx);
        assert(val == int_str);
    }
}

void SmallPool()
{
    kvstore::kv_options.index_buffer_pool_size = 3;

    InitData();
}

thread_local std::unique_ptr<kvstore::IndexPageManager> kvstore::idx_manager =
    nullptr;
thread_local std::unique_ptr<kvstore::StorageManager> kvstore::storage_manager =
    nullptr;

int main()
{
    kvstore::storage_manager = std::make_unique<kvstore::StorageManager>();
    kvstore::idx_manager = std::make_unique<kvstore::IndexPageManager>(
        &kvstore::kv_options, kvstore::storage_manager->GetIoManager());

    // BuildIter(&options);
    // Upsert();
    SmallPool();
}