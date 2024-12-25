#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "error.h"
#include "index_page_manager.h"
#include "read_task.h"
#include "scan_task.h"
#include "storage_manager.h"
#include "table_ident.h"
#include "write_task.h"

thread_local std::unique_ptr<kvstore::IndexPageManager> kvstore::idx_manager =
    nullptr;
thread_local std::unique_ptr<kvstore::StorageManager> kvstore::storage_manager =
    nullptr;

void InitEnv()
{
    if (kvstore::storage_manager == nullptr)
    {
        kvstore::storage_manager = std::make_unique<kvstore::StorageManager>();
        kvstore::idx_manager = std::make_unique<kvstore::IndexPageManager>(
            &kvstore::kv_options, kvstore::storage_manager->GetIoManager());
    }
    else
    {
        assert(kvstore::idx_manager != nullptr);
    }
}

void InitData(const std::string &tbl_name,
              uint32_t partition_id,
              size_t data_size,
              uint64_t data_ts)
{
    kvstore::WriteTask write_task{tbl_name, partition_id, &kvstore::kv_options};
    kvstore::thd_task = &write_task;
    write_task.Reset(kvstore::idx_manager.get());

    for (size_t idx = 0; idx < data_size; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           data_ts,
                           kvstore::WriteOp::Upsert);
    }

    write_task.Apply();
}

MapVerifier::MapVerifier(kvstore::TableIdent tid) : tid_(tid)
{
    InitEnv();
}

MapVerifier::~MapVerifier()
{
    if (!answer_.empty())
    {
        Clean();
    }
}

void MapVerifier::Upsert(uint64_t begin, uint64_t end)
{
    std::cout << "Upsert(" << begin << ',' << end << ')' << std::endl;
    ts_++;

    kvstore::WriteTask wtask{
        tid_.tbl_name_, tid_.partition_id_, &kvstore::kv_options};
    kvstore::thd_task = &wtask;
    wtask.Reset(kvstore::idx_manager.get());
    for (size_t idx = begin; idx < end; ++idx)
    {
        wtask.AddData(
            Key(idx), std::to_string(idx), ts_, kvstore::WriteOp::Upsert);
        answer_.insert_or_assign(
            Key(idx), kvstore::Tuple(Key(idx), std::to_string(idx), ts_));
    }
    wtask.Apply();

    if (auto_validate_)
    {
        Validate();
    }
}

void MapVerifier::Delete(uint64_t begin, uint64_t end)
{
    std::cout << "Delete(" << begin << ',' << end << ')' << std::endl;
    ts_++;

    kvstore::WriteTask wtask{
        tid_.tbl_name_, tid_.partition_id_, &kvstore::kv_options};
    kvstore::thd_task = &wtask;
    wtask.Reset(kvstore::idx_manager.get());
    for (size_t idx = begin; idx < end; ++idx)
    {
        wtask.AddData(Key(idx), "", ts_, kvstore::WriteOp::Delete);
        answer_.erase(Key(idx));
    }
    wtask.Apply();

    if (auto_validate_)
    {
        Validate();
    }
}

void MapVerifier::WriteRandom(uint64_t begin,
                              uint64_t end,
                              uint8_t del,
                              uint8_t density)
{
    constexpr uint8_t max = 100;
    del = del > max ? max : del;
    density = density > max ? max : density;
    std::cout << "WriteRandom(" << begin << ',' << end << ',' << int(del) << ','
              << int(density) << ')' << std::endl;
    ts_++;
    srand(ts_);

    kvstore::WriteTask wtask{
        tid_.tbl_name_, tid_.partition_id_, &kvstore::kv_options};
    kvstore::thd_task = &wtask;
    wtask.Reset(kvstore::idx_manager.get());
    for (size_t idx = begin; idx < end; ++idx)
    {
        if ((rand() % max) >= density)
        {
            continue;
        }
        if ((rand() % max) < del)
        {
            wtask.AddData(Key(idx), "", ts_, kvstore::WriteOp::Delete);
            answer_.erase(Key(idx));
        }
        else
        {
            wtask.AddData(
                Key(idx), std::to_string(idx), ts_, kvstore::WriteOp::Upsert);
            answer_.insert_or_assign(
                Key(idx), kvstore::Tuple(Key(idx), std::to_string(idx), ts_));
        }
    }
    wtask.Apply();

    if (auto_validate_)
    {
        Validate();
    }
}

void MapVerifier::Clean()
{
    std::cout << "Clean()" << std::endl;
    ts_++;

    kvstore::WriteTask wtask{
        tid_.tbl_name_, tid_.partition_id_, &kvstore::kv_options};
    kvstore::thd_task = &wtask;
    wtask.Reset(kvstore::idx_manager.get());
    for (auto [k, _] : answer_)
    {
        wtask.AddData(k, "", ts_, kvstore::WriteOp::Delete);
    }
    wtask.Apply();

    answer_.clear();

    if (auto_validate_)
    {
        Validate();
    }
}

void MapVerifier::Read(uint64_t k)
{
    std::cout << "Read(" << k << ')' << std::endl;
    kvstore::ReadTask read_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &read_task;
    std::string_view val;
    uint64_t ts;
    kvstore::KvError err = read_task.Read(tid_, Key(k), val, ts);
    if (err != kvstore::KvError::NotFound)
    {
        REQUIRE(err == kvstore::KvError::NoError);
        REQUIRE(answer_.at(Key(k)) == kvstore::Tuple(Key(k), val, ts));
    }
}

void MapVerifier::Scan(uint64_t begin, uint64_t end)
{
    std::cout << "Scan(" << begin << ',' << end << ')' << std::endl;
    kvstore::ScanTask scan_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &scan_task;
    std::vector<kvstore::Tuple> tuples;
    kvstore::KvError err =
        scan_task.ScanVec(tid_, Key(begin), Key(end), tuples);
    REQUIRE(err == kvstore::KvError::NoError);
    auto it = answer_.lower_bound(Key(begin));
    for (auto &t : tuples)
    {
        assert(t == it->second);
        it++;
    }
}

void MapVerifier::Validate()
{
    kvstore::ScanTask scan_task{kvstore::idx_manager.get()};
    kvstore::thd_task = &scan_task;
    std::vector<kvstore::Tuple> tuples;
    kvstore::KvError err =
        scan_task.ScanVec(tid_, std::string_view{}, std::string_view{}, tuples);
    if (err == kvstore::KvError::NotFound)
    {
        REQUIRE(answer_.empty());
        REQUIRE(tuples.empty());
        return;
    }
    REQUIRE(err == kvstore::KvError::NoError);
    auto it = answer_.begin();
    for (auto &t : tuples)
    {
        assert(t == it->second);
        it++;
    }
    REQUIRE(it == answer_.end());
}

std::string MapVerifier::Key(uint64_t k)
{
    constexpr int sz = 16;
    std::stringstream ss;
    ss << std::setw(sz) << std::setfill('0') << k;
    std::string kstr = ss.str();
    assert(kstr.size() == sz);
    return kstr;
}

void MapVerifier::SetAutoValidate(bool v)
{
    auto_validate_ = v;
}
