#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "error.h"
#include "page_mapper.h"
#include "shard.h"

namespace eloqstore
{
ScanIterator::ScanIterator(const TableIdent &tbl_id)
    : tbl_id_(tbl_id), iter_(nullptr, Options())
{
}

KvError ScanIterator::Seek(std::string_view key, bool ttl)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id_);
    CHECK_KV_ERR(err);
    PageId root_id = ttl ? meta->ttl_root_id_ : meta->root_id_;
    if (root_id == MaxPageId)
    {
        return KvError::EndOfFile;
    }
    compression_ = meta->compression_.get();
    mapping_ = meta->mapper_->GetMappingSnapshot();

    err = PrefetchPages(root_id, key);
    CHECK_KV_ERR(err);

    if (!iter_.Seek(key))
    {
        err = Next();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError ScanIterator::Next()
{
    if (!iter_.HasNext())
    {
        if (prefetched_offset_ < prefetched_pages_.size())
        {
            data_page_ = std::move(prefetched_pages_[prefetched_offset_]);
            ++prefetched_offset_;
        }
        else
        {
            PageId page_id = data_page_.NextPageId();
            if (page_id == MaxPageId)
            {
                return KvError::EndOfFile;
            }
            FilePageId file_page = mapping_->ToFilePage(page_id);
            assert(file_page != MaxFilePageId);
            auto [page, err] = LoadDataPage(tbl_id_, page_id, file_page);
            CHECK_KV_ERR(err);

            data_page_ = std::move(page);
        }
        iter_.Reset(&data_page_, Options()->data_page_size);
        assert(iter_.HasNext());
    }
    iter_.Next();
    return KvError::NoError;
}

std::string_view ScanIterator::Key() const
{
    return iter_.Key();
}

std::pair<std::string_view, KvError> ScanIterator::ResolveValue(
    std::string &storage)
{
    return eloqstore::ResolveValue(
        tbl_id_, mapping_.get(), iter_, storage, compression_);
}

uint64_t ScanIterator::ExpireTs() const
{
    return iter_.ExpireTs();
}

uint64_t ScanIterator::Timestamp() const
{
    return iter_.Timestamp();
}

MappingSnapshot *ScanIterator::Mapping() const
{
    return mapping_.get();
}

KvError ScanIterator::PrefetchPages(PageId root_id, std::string_view key)
{
    std::vector<PageId> page_ids;
    KvError err = shard->IndexManager()->SeekIndexMultiplePages(
        mapping_.get(), root_id, key, kPrefetchPageCount, page_ids);
    CHECK_KV_ERR(err);
    assert(!page_ids.empty());

    prefetched_pages_.reserve(page_ids.size());
    std::vector<FilePageId> file_page_ids;
    file_page_ids.reserve(page_ids.size());

    for (PageId page_id : page_ids)
    {
        file_page_ids.emplace_back(mapping_->ToFilePage(page_id));
    }

    std::vector<Page> data_pages;
    err = IoMgr()->ReadPages(tbl_id_, file_page_ids, data_pages);
    CHECK_KV_ERR(err);

    for (size_t i = 0; i < data_pages.size(); ++i)
    {
        prefetched_pages_.emplace_back(page_ids[i], std::move(data_pages[i]));
    }

    data_page_ = std::move(prefetched_pages_[prefetched_offset_]);
    ++prefetched_offset_;
    iter_.Reset(&data_page_, Options()->data_page_size);
    return KvError::NoError;
}

KvError ScanTask::Scan()
{
    const TableIdent &tbl_id = req_->TableId();
    auto req = static_cast<ScanRequest *>(req_);
    assert(req->page_entries_ > 0 && req->page_size_ > 0);
    req->num_entries_ = 0;
    req->has_remaining_ = false;
    size_t result_size = 0;

    ScanIterator iter(tbl_id);
    KvError err = iter.Seek(req->BeginKey());
    if (err != KvError::NoError)
    {
        return err == KvError::EndOfFile ? KvError::NoError : err;
    }

    if (!req->begin_inclusive_ &&
        Comp()->Compare(iter.Key(), req->BeginKey()) == 0)
    {
        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }

    std::string value_storage;
    while (req->EndKey().empty() ||
           Comp()->Compare(iter.Key(), req->EndKey()) < 0)
    {
        // Check entries number limit.
        if (req->num_entries_ == req->page_entries_)
        {
            req->has_remaining_ = true;
            break;
        }

        // Fetch value
        auto [value, fetch_err] = iter.ResolveValue(value_storage);
        err = fetch_err;
        assert(err != KvError::EndOfFile);
        CHECK_KV_ERR(err);

        // Check result size limit.
        const size_t entry_size = iter.Key().size() + value.size() +
                                  sizeof(iter.Timestamp()) +
                                  sizeof(iter.ExpireTs());
        if (result_size > 0 && result_size + entry_size > req->page_size_)
        {
            req->has_remaining_ = true;
            break;
        }
        result_size += entry_size;

        KvEntry &entry = req->num_entries_ < req->entries_.size()
                             ? req->entries_[req->num_entries_]
                             : req->entries_.emplace_back();
        req->num_entries_++;
        entry.key_.assign(iter.Key());
        entry.value_ = value_storage.empty() ? value : std::move(value_storage);
        entry.timestamp_ = iter.Timestamp();
        entry.expire_ts_ = iter.ExpireTs();

        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }
    return KvError::NoError;
}
}  // namespace eloqstore
