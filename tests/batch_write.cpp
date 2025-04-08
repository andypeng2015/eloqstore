#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

TEST_CASE("batch entry with smaller timestamp", "[batch_write]")
{
    // TODO:
    // Input batch entry of write has smaller timestamp than existing kv entry.
}

#ifndef NDEBUG
TEST_CASE("batch write arguments", "[batch_write]")
{
    // TODO: Batch write with duplicated or disordered keys
}
#endif