#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>

#include "error.h"
#include "index_page_manager.h"
#include "page_mapper.h"

namespace kvstore
{
ScanTask::ScanTask() : iter_(nullptr, Options())
{
}

KvError ScanTask::Scan(const TableIdent &tbl_id,
                       std::string_view begin_key,
                       std::string_view end_key,
                       std::vector<KvEntry> &entries)
{
    auto [root, mapper, err] = index_mgr->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = mapper->GetMappingSnapshot();

    uint32_t page_id;
    err = index_mgr->SeekIndex(mapping.get(), tbl_id, root, begin_key, page_id);
    CHECK_KV_ERR(err);
    uint32_t file_page = mapping->ToFilePage(page_id);
    data_page_.Init(page_id, Options()->data_page_size);
    err = IoMgr()->ReadPage(tbl_id, file_page, data_page_.PagePtrPtr());
    CHECK_KV_ERR(err);

    iter_.Reset(&data_page_, Options()->data_page_size);
    iter_.Seek(begin_key);
    if (iter_.Key().empty() && (err = Next(mapping.get())) != KvError::NoError)
    {
        return err == KvError::EndOfFile ? KvError::NoError : err;
    }
    while (end_key.empty() || iter_.Key() < end_key)
    {
        entries.emplace_back(iter_.Key(), iter_.Value(), iter_.Timestamp());
        if ((err = Next(mapping.get())) != KvError::NoError)
        {
            break;
        }
    }
    return err == KvError::EndOfFile ? KvError::NoError : err;
}

KvError ScanTask::NextPage(MappingSnapshot *m)
{
    uint32_t page_id = data_page_.NextPageId();
    if (page_id == UINT32_MAX)
    {
        // EndOfFile will just break the scan process
        return KvError::EndOfFile;
    }
    uint32_t file_page = m->ToFilePage(page_id);
    data_page_.Init(page_id, Options()->data_page_size);
    KvError err =
        IoMgr()->ReadPage(*m->tbl_ident_, file_page, data_page_.PagePtrPtr());
    CHECK_KV_ERR(err);

    iter_.Reset(&data_page_, Options()->data_page_size);
    return KvError::NoError;
}

KvError ScanTask::Next(MappingSnapshot *m)
{
    if (!iter_.HasNext())
    {
        KvError err = NextPage(m);
        CHECK_KV_ERR(err);
    }
    bool ok = iter_.Next();
    assert(ok && !iter_.Key().empty());
    return KvError::NoError;
}

KvError ScanTask::Iterate(const TableIdent &tbl_id,
                          std::string_view begin_key,
                          std::string_view end_key)
{
    end_key_ = end_key;
    auto [root, mapper, err] = index_mgr->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    mapping_ = mapper->GetMappingSnapshot();

    uint32_t page_id;
    err =
        index_mgr->SeekIndex(mapping_.get(), tbl_id, root, begin_key, page_id);
    CHECK_KV_ERR(err);
    uint32_t file_page = mapping_->ToFilePage(page_id);
    data_page_.Init(page_id, Options()->data_page_size);
    err = IoMgr()->ReadPage(tbl_id, file_page, data_page_.PagePtrPtr());
    CHECK_KV_ERR(err);

    iter_.Reset(&data_page_, Options()->data_page_size);
    iter_.Seek(begin_key);
    if (iter_.Key().empty())
    {
        err = Next(mapping_.get());
    }
    return err;
}

bool ScanTask::Valid() const
{
    return !iter_.Key().empty() && (end_key_.empty() || iter_.Key() < end_key_);
}

std::string_view ScanTask::Key() const
{
    return iter_.Key();
}

std::string_view ScanTask::Value() const
{
    return iter_.Value();
}

uint64_t ScanTask::Timestamp() const
{
    return iter_.Timestamp();
}

}  // namespace kvstore