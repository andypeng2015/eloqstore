#include "read_task.h"

#include "error.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"

namespace kvstore
{

void ReadTask::Reset(IndexPageManager *idx_page_manager)
{
}

KvError ReadTask::Read(const TableIdent &tbl_ident,
                       std::string_view search_key,
                       std::string_view &value,
                       uint64_t &timestamp)
{
    auto [root, mapper, err] = index_mgr->FindRoot(tbl_ident);
    CHECK_KV_ERR(err);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = mapper->GetMappingSnapshot();

    uint32_t data_page_id;
    err = index_mgr->SeekIndex(
        mapping.get(), tbl_ident, root, search_key, data_page_id);
    CHECK_KV_ERR(err);
    uint32_t file_page = mapping->ToFilePage(data_page_id);
    data_page_.Init(data_page_id, Options()->data_page_size);
    err = IoMgr()->ReadPage(tbl_ident, file_page, data_page_.PagePtrPtr());
    CHECK_KV_ERR(err);

    DataPageIter data_iter{&data_page_, Options()};
    data_iter.Seek(search_key);
    std::string_view seek_key = data_iter.Key();
    if (!seek_key.empty() && seek_key == search_key)
    {
        value = data_iter.Value();
        timestamp = data_iter.Timestamp();
        return KvError::NoError;
    }
    else
    {
        return KvError::NotFound;
    }
}

}  // namespace kvstore