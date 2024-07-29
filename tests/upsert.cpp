#include <catch2/catch_test_macros.hpp>

#include "common.h"
#include "read_task.h"
#include "write_task.h"

TEST_CASE("upsert", "[replace]")
{
    InitEnv();

    std::string tbl_name{"t1"};
    uint32_t partition_id = 0;
    size_t n = 1000;
    uint64_t base_ts = 2;

    InitData(tbl_name, partition_id, n, base_ts);

    kvstore::WriteTask write_task{tbl_name, partition_id, &kvstore::kv_options};
    kvstore::thd_task = &write_task;
    write_task.Reset(kvstore::idx_manager.get());

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
        REQUIRE(err == kvstore::KvError::NoError);
        std::string int_str = std::to_string(idx);
        REQUIRE(val == int_str);
        REQUIRE(ts == base_ts);
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
        REQUIRE(err == kvstore::KvError::NoError);
        size_t p_idx = idx;
        if (idx >= 100 && idx < 200)
        {
            p_idx = idx * 2;
        }
        std::string int_str = std::to_string(p_idx);
        REQUIRE(val == int_str);
    }
}