#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>
#include <unistd.h>

#include "eloq_store.h"

#ifndef __AFL_INIT
#define __AFL_INIT() ((void)0)
#endif

#ifndef __AFL_LOOP
static inline int __AFL_LOOP(unsigned int) noexcept
{
    return 1;
}
#endif

namespace fs = std::filesystem;

namespace
{
constexpr size_t kMaxInputSize = 4096;
constexpr size_t kMaxKeySize = 24;
constexpr size_t kMaxValueSize = 96;

struct Cursor
{
    Cursor(const uint8_t *data, size_t len) : data_(data), len_(len) {}

    size_t Remaining() const
    {
        return len_ - pos_;
    }

    uint8_t TakeU8()
    {
        if (pos_ >= len_)
        {
            return 0;
        }
        return data_[pos_++];
    }

    uint64_t TakeU64()
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
        {
            v = (v << 8U) | TakeU8();
        }
        return v;
    }

    std::string TakeString(size_t max_len)
    {
        if (Remaining() == 0)
        {
            return {};
        }
        size_t want = TakeU8() % (max_len + 1);
        want = std::min(want, Remaining());
        std::string out;
        out.reserve(want);
        for (size_t i = 0; i < want && pos_ < len_; ++i)
        {
            out.push_back(static_cast<char>('a' + (data_[pos_++] % 26)));
        }
        return out;
    }

private:
    const uint8_t *data_;
    size_t len_;
    size_t pos_{0};
};

void DoWrite(eloqstore::EloqStore &store,
             const eloqstore::TableIdent &tbl,
             Cursor &cursor,
             eloqstore::WriteOp op)
{
    std::string key = cursor.TakeString(kMaxKeySize);
    if (key.empty())
    {
        key = "k";
    }
    std::string val;
    if (op == eloqstore::WriteOp::Upsert)
    {
        val = cursor.TakeString(kMaxValueSize);
    }
    uint64_t ts = cursor.TakeU64();
    uint64_t expire_ts = (cursor.TakeU8() & 1U) != 0 ? cursor.TakeU64() : 0;

    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> batch;
    batch.emplace_back(std::move(key), std::move(val), ts, op, expire_ts);
    req.SetArgs(tbl, std::move(batch));
    store.ExecSync(&req);
}

void DoRead(eloqstore::EloqStore &store,
            const eloqstore::TableIdent &tbl,
            Cursor &cursor)
{
    std::string key = cursor.TakeString(kMaxKeySize);
    if (key.empty())
    {
        return;
    }
    eloqstore::ReadRequest req;
    req.SetArgs(tbl, key);
    store.ExecSync(&req);
}

void DoScan(eloqstore::EloqStore &store,
            const eloqstore::TableIdent &tbl,
            Cursor &cursor)
{
    std::string begin = cursor.TakeString(kMaxKeySize);
    std::string end = cursor.TakeString(kMaxKeySize);
    if (begin.empty() && end.empty())
    {
        return;
    }
    if (end.empty())
    {
        end = begin;
        end.push_back('z');
    }
    if (begin >= end)
    {
        std::swap(begin, end);
    }

    eloqstore::ScanRequest req;
    req.SetArgs(tbl, begin, end);
    req.SetPagination(8, 4 * 1024);
    req.SetPrefetchPageNum(1 + (cursor.TakeU8() % 4));
    store.ExecSync(&req);
}

void RunOneInput(const uint8_t *data, size_t len)
{
    if (data == nullptr || len == 0)
    {
        return;
    }

    Cursor cursor(data, len);
    uint64_t salt = cursor.TakeU64();

    fs::path base = fs::temp_directory_path() / "eloq_afl";
    fs::path workdir = base / std::to_string(salt);
    std::error_code ec;
    fs::create_directories(workdir, ec);

    eloqstore::KvOptions opts;
    opts.num_threads = 1;
    opts.store_path = {workdir.string()};
    opts.fd_limit = 64;
    opts.io_queue_size = 256;
    opts.max_write_batch_pages = 32;
    opts.pages_per_file_shift = 12;
    opts.data_append_mode = false;

    if (!eloqstore::EloqStore::ValidateOptions(opts))
    {
        fs::remove_all(workdir, ec);
        return;
    }

    eloqstore::EloqStore store(opts);
    if (store.Start() != eloqstore::KvError::NoError)
    {
        fs::remove_all(workdir, ec);
        return;
    }

    eloqstore::TableIdent tbl("afl_tbl", static_cast<uint32_t>(cursor.TakeU8()));
    size_t op_budget = 1 + (cursor.TakeU8() % 32);
    for (size_t i = 0; i < op_budget && cursor.Remaining() > 0; ++i)
    {
        uint8_t op = cursor.TakeU8();
        switch (op % 4)
        {
        case 0:
            DoWrite(store, tbl, cursor, eloqstore::WriteOp::Upsert);
            break;
        case 1:
            DoWrite(store, tbl, cursor, eloqstore::WriteOp::Delete);
            break;
        case 2:
            DoRead(store, tbl, cursor);
            break;
        default:
            DoScan(store, tbl, cursor);
            break;
        }
    }

    store.Stop();
    fs::remove_all(workdir, ec);
}
}  // namespace

int main()
{
    __AFL_INIT();
    std::vector<uint8_t> buf(kMaxInputSize);
    while (__AFL_LOOP(1000))
    {
        ssize_t len = read(STDIN_FILENO, buf.data(), buf.size());
        if (len <= 0)
        {
            break;
        }
        RunOneInput(buf.data(), static_cast<size_t>(len));
    }
    return 0;
}
