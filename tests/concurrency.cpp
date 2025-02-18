#include <glog/logging.h>

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "common.h"
#include "table_ident.h"

TEST_CASE("concurrent tasks with memory store", "[concurrency]")
{
    InitMemStore();
    ConcurrentTester tester(memstore.get(), test_tbl_id, 16, 32, 20);
    tester.Init();
    tester.Run(5);
}
