#include <catch2/catch_test_macros.hpp>

#include "tests/common.h"

TEST_CASE("simple delete", "[delete]")
{
    MapVerifier verify(kvstore::TableIdent{"t1", 1});
    verify.Upsert(100, 300);
    verify.Delete(150, 200);
    verify.Upsert(200, 230);
    verify.Delete(0, 100);
    verify.Delete(100, 500);
    verify.Upsert(1000, 2000);
    verify.Delete(500, 1200);
}

TEST_CASE("clean data", "[delete]")
{
    constexpr uint64_t max_val = 1000;
    MapVerifier verify(kvstore::TableIdent{"t1", 1});
    verify.Delete(0, 100);
    verify.SetAutoValidate(false);
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRandom(1, max_val, 0, 20);
    }
    verify.Clean();
    verify.Validate();
}

TEST_CASE("decrease height", "[delete]")
{
    MapVerifier verify(kvstore::TableIdent{"t1", 1});
    verify.Upsert(1, 1000);
    for (int i = 0; i < 1000; i += 50)
    {
        verify.Delete(i, i + 50);
    }
}

TEST_CASE("random upsert/delete and scan", "[delete]")
{
    constexpr uint64_t max_val = 100000;
    MapVerifier verify(kvstore::TableIdent{"t1", 1});
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRandom(1, max_val, 20, 30);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}