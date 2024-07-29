#pragma once

#include <cstdint>

#include "circular_queue.h"
#include "mem_storage.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class WriteTask;
class WriteReq;

class AsyncIoManager
{
public:
    AsyncIoManager();

    void Read(const TableIdent &tbl_ident,
              uint32_t file_page_id,
              char *ptr,
              size_t size);

    void Write(WriteReq *write_req, bool commit = false);

    void PollReads();
    void PollWrites();

private:
    void FakeWrite(WriteReq *write_req);
    KvError FakeRead(const TableIdent &tbl_ident,
                     uint32_t page_id,
                     char *ptr,
                     size_t size);

    struct FakeIoReq
    {
        const TableIdent *tbl_ident_;
        uint32_t file_page_id_;
        char *ptr_;
        size_t size_;
        KvTask *task_;
    };

    CircularQueue<FakeIoReq> fake_read_ring_;
    CircularQueue<WriteReq *> fake_write_ring_;

    uint32_t read_cnt_{0};
    uint32_t write_cnt_{0};

    CircularQueue<KvTask *> read_waiting_zone_;
    CircularQueue<WriteTask *> write_waiting_zone_;

    MemStorage mem_store_;
};
}  // namespace kvstore