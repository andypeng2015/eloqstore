#include <catch2/catch_test_macros.hpp>

#include "common.h"
#include "global_variables.h"
#include "read_task.h"

TEST_CASE("insert", "[init]")
{
    InitEnv();

    std::string tbl_name{"t1"};
    uint32_t partition_id = 0;
    size_t n = 1000;
    uint64_t base_ts = 2;

    InitData(tbl_name, partition_id, n, base_ts);

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

        REQUIRE(err == kvstore::KvError::NoError);
        std::string int_str = std::to_string(idx);
        REQUIRE(val == int_str);
        REQUIRE(ts == base_ts);
    }
}