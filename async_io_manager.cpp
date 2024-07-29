#include "async_io_manager.h"

#include <cassert>

#include "global_variables.h"
#include "read_task.h"
#include "write_task.h"

namespace kvstore
{
AsyncIoManager::AsyncIoManager()
    : fake_write_ring_(kv_options.io_write_buffer_size)
{
}

void AsyncIoManager::Read(const TableIdent &tbl_ident,
                          uint32_t file_page_id,
                          char *ptr,
                          size_t size)
{
    if (thd_task->Type() == TaskType::Read)
    {
        ReadTask *read_task = static_cast<ReadTask *>(thd_task);
        read_task->read_fps_.emplace(file_page_id);
    }

    while (fake_read_ring_.Size() >= kv_options.io_read_buffer_size)
    {
        // Too many read IO requests.
        read_waiting_zone_.Enqueue(thd_task);
        thd_task->Yield();
    }
    fake_read_ring_.Enqueue(
        FakeIoReq{&tbl_ident, file_page_id, ptr, size, thd_task});

    thd_task->status_ = TaskStatus::BlockedForOne;
    thd_task->Yield();

    PollReads();
}

void AsyncIoManager::Write(WriteReq *write_req, bool commit)
{
    WriteTask *task = write_req->task_;
    assert(task == static_cast<WriteTask *>(thd_task));

    while (fake_write_ring_.Size() >= kv_options.io_write_buffer_size)
    {
        write_waiting_zone_.Enqueue(task);
        thd_task->Yield();
    }

    fake_write_ring_.Enqueue(write_req);

    if (commit)
    {
        thd_task->status_ = TaskStatus::BlockedForAll;
        thd_task->Yield();
    }
    else
    {
        // A write task is allowed to issue multiple write requests and is only
        // blocked if this is the last.
    }

    PollWrites();
}

void AsyncIoManager::PollReads()
{
    size_t finish_cnt = fake_read_ring_.Size();
    while (fake_read_ring_.Size() > 0)
    {
        FakeIoReq &req = fake_read_ring_.Peek();
        KvError err =
            FakeRead(*req.tbl_ident_, req.file_page_id_, req.ptr_, req.size_);
        KvTask *task = req.task_;
        fake_read_ring_.Dequeue();
        task->kv_error_ = err;

        size_t read_remaining = 0;
        if (task->Type() == TaskType::Read)
        {
            ReadTask *read_task = static_cast<ReadTask *>(task);
            read_task->read_fps_.erase(req.file_page_id_);
            read_remaining = read_task->read_fps_.size();
        }

        if (task->status_ == TaskStatus::BlockedForOne)
        {
            // The task is waiting for one request to finish. Resumes the
            // task.
            task->Resume();
        }
        else if (task->status_ == TaskStatus::BlockedForAll &&
                 read_remaining == 0)
        {
            // The task is waiting for all requests to finish.
            task->Resume();
        }
        task->status_ = TaskStatus::Ongoing;
    }

    // Some read requests have finished. Resumes tasks in the waiting zone to
    // issue IO requests.
    size_t idx = 0;
    while (idx < finish_cnt && read_waiting_zone_.Size() > 0)
    {
        ++idx;
        KvTask *wait_task = read_waiting_zone_.Peek();
        read_waiting_zone_.Dequeue();
        wait_task->Resume();
    }
}

void AsyncIoManager::PollWrites()
{
    size_t finish_cnt = fake_write_ring_.Size();
    while (fake_write_ring_.Size() > 0)
    {
        WriteReq *req = fake_write_ring_.Peek();
        WriteTask *task = req->task_;
        fake_write_ring_.Dequeue();

        FakeWrite(req);

        // The write request has finished.
        task->FinishIo(req);

        if (task->status_ == TaskStatus::BlockedForOne ||
            (task->status_ == TaskStatus::BlockedForAll &&
             task->IsWriteBufferEmpty()))
        {
            task->status_ = TaskStatus::Ongoing;
            task->Resume();
        }
    }

    // Some write requests have finished. Resumes tasks in the waiting zone.
    size_t idx = 0;
    while (idx < finish_cnt && write_waiting_zone_.Size() > 0)
    {
        ++idx;
        WriteTask *wait_task = write_waiting_zone_.Peek();
        write_waiting_zone_.Dequeue();
        wait_task->Resume();
    }
}

void AsyncIoManager::FakeWrite(WriteReq *write_req)
{
    const char *ptr = nullptr;
    size_t size = 0;

    if (write_req->page_.index() == 0)
    {
        MemIndexPage *mem_page = std::get<0>(write_req->page_);
        ptr = mem_page->PagePtr();
        size = kv_options.index_page_size;
    }
    else
    {
        DataPage *data_page = std::get<1>(write_req->page_);
        ptr = data_page->PagePtr();
        size = kv_options.data_page_size;
    }

    KvError err = mem_store_.Write(
        *write_req->tbl_ident_, write_req->file_page_id_, ptr, size);
    thd_task->kv_error_ = err;
}

KvError AsyncIoManager::FakeRead(const TableIdent &tbl_ident,
                                 uint32_t page_id,
                                 char *ptr,
                                 size_t size)
{
    return mem_store_.Read(tbl_ident, page_id, ptr, size);
}
}  // namespace kvstore