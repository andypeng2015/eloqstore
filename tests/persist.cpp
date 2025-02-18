#include <glog/logging.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include "eloq_store.h"
#include "kv_options.h"
#include "table_ident.h"
#include "tests/common.h"

static std::unique_ptr<kvstore::EloqStore> eloqstore = nullptr;
constexpr char test_path[] = "/tmp/eloqstore";
kvstore::KvOptions opts = {
    .data_path = test_path,
};

void InitStore()
{
    if (eloqstore)
    {
        return;
    }

    if (std::filesystem::exists(test_path))
    {
        std::filesystem::remove_all(test_path);
    }

    eloqstore = std::make_unique<kvstore::EloqStore>(opts);
    eloqstore->Start();
}

TEST_CASE("simple persist", "[persist]")
{
    InitStore();
    MapVerifier verify(test_tbl_id, eloqstore.get());
    verify.Upsert(100, 200);
    verify.Delete(100, 150);
    verify.Upsert(0, 50);
    verify.WriteRnd(0, 200);
    verify.WriteRnd(0, 200);
}

TEST_CASE("complex persist", "[persist]")
{
    InitStore();
    MapVerifier verify(test_tbl_id, eloqstore.get());
    for (int i = 0; i < 5; i++)
    {
        verify.WriteRnd(0, 2000);
    }
}

TEST_CASE("persist with restart", "[persist]")
{
    InitStore();

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 3; i++)
    {
        kvstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, eloqstore.get()));
    }

    for (int i = 0; i < 5; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->WriteRnd(0, 1000);
        }
        eloqstore->Stop();
        eloqstore = nullptr;

        eloqstore = std::make_unique<kvstore::EloqStore>(opts);
        eloqstore->Start();
        for (auto &tbl : tbls)
        {
            tbl->SetStore(eloqstore.get());
        }
    }
}

TEST_CASE("easy concurrent tasks with iouring", "[concurrency]")
{
    InitStore();
    ConcurrentTester tester(eloqstore.get(), test_tbl_id, 16, 32, 20);
    tester.Init();
    tester.Run(5);
}

TEST_CASE("hard concurrent tasks with iouring", "[concurrency][slow]")
{
    InitStore();
    ConcurrentTester tester(eloqstore.get(), test_tbl_id, 32, 8192, 1000);
    tester.Init();
    tester.Run(5);
}

TEST_CASE("simple LRU for opened fd", "[persist]")
{
    char path[] = "/tmp/eloqstore_fd_lru";
    std::filesystem::remove_all(path);

    kvstore::KvOptions opts1 = opts;
    opts1.data_path = path;
    opts1.fd_limit = 12;
    opts1.data_page_size = kvstore::page_align;
    opts1.data_file_pages = 1;
    kvstore::EloqStore store1(opts1);
    store1.Start();

    MapVerifier verify(test_tbl_id, &store1);
    verify.Upsert(1, 5000);
    verify.Upsert(5000, 10000);

    verify.Upsert(1, 10000);
}

TEST_CASE("complex LRU for opened fd", "[persist]")
{
    char path[] = "/tmp/eloqstore_fd_lru";
    std::filesystem::remove_all(path);

    kvstore::KvOptions opts1 = opts;
    opts1.data_path = path;
    opts1.fd_limit = 12;
    opts1.data_page_size = kvstore::page_align;
    opts1.data_file_pages = 1;
    kvstore::EloqStore store1(opts1);
    store1.Start();

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 10; i++)
    {
        kvstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, &store1));
    }

    for (uint32_t i = 0; i < 3; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->Upsert(0, 5000);
        }
    }
}
