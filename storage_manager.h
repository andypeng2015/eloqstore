#pragma once

#include "async_io_manager.h"

namespace kvstore
{
class WriteTask;

class StorageManager
{
public:
    StorageManager();

    void Read(char *ptr,
              size_t size,
              const TableIdent &tbl_ident,
              uint32_t file_page_id);

    void Write(WriteReq *write_req, bool commit = false);

    AsyncIoManager *GetIoManager()
    {
        return &io_manager_;
    }

private:
    AsyncIoManager io_manager_;
};
}  // namespace kvstore