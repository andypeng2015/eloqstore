#include "storage_manager.h"

#include "global_variables.h"
#include "write_task.h"

namespace kvstore
{
StorageManager::StorageManager()
{
}

void StorageManager::Read(char *ptr,
                          size_t size,
                          const TableIdent &tbl_ident,
                          uint32_t file_page_id)
{
    io_manager_.Read(tbl_ident, file_page_id, ptr, size);
}

void StorageManager::Write(WriteReq *write_req, bool commit)
{
    assert(write_req->page_id_ != UINT32_MAX);
    assert(write_req->file_page_id_ != UINT32_MAX);
    io_manager_.Write(write_req, commit);
}
}  // namespace kvstore