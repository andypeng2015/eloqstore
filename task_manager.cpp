#include "task_manager.h"

#include <boost/context/continuation_fcontext.hpp>
#include <cassert>
#include <utility>

#include "read_task.h"
#include "task.h"
#include "write_task.h"

using namespace boost::context;

namespace kvstore
{
WriteTask *TaskManager::GetWriteTask(const TableIdent &tbl_id)
{
    if (writing_.find(tbl_id) != writing_.end())
    {
        return nullptr;
    }
    if (free_write_.empty())
    {
        auto task = std::make_unique<WriteTask>(tbl_id);
        free_write_.push_back(task.get());
        write_tasks_.emplace_back(std::move(task));
    }
    WriteTask *task = free_write_.back();
    free_write_.pop_back();
    assert(task->status_ == TaskStatus::Idle);
    writing_.emplace(tbl_id, task);
    task->Reset(tbl_id);
    return task;
}

ReadTask *TaskManager::GetReadTask()
{
    if (free_read_.empty())
    {
        auto task = std::make_unique<ReadTask>();
        free_read_.push_back(task.get());
        read_tasks_.emplace_back(std::move(task));
    }
    ReadTask *task = free_read_.back();
    free_read_.pop_back();
    return task;
}

ScanTask *TaskManager::GetScanTask()
{
    if (free_scan_.empty())
    {
        auto task = std::make_unique<ScanTask>();
        free_scan_.push_back(task.get());
        scan_tasks_.emplace_back(std::move(task));
    }
    ScanTask *task = free_scan_.back();
    free_scan_.pop_back();
    return task;
}

void TaskManager::FreeTask(KvTask *task)
{
    switch (task->Type())
    {
    case TaskType::Read:
        free_read_.push_back(static_cast<ReadTask *>(task));
        break;
    case TaskType::Scan:
        free_scan_.push_back(static_cast<ScanTask *>(task));
        break;
    case TaskType::Write:
    {
        WriteTask *wtask = static_cast<WriteTask *>(task);
        writing_.erase(wtask->TableId());
        free_write_.push_back(wtask);
        break;
    }
    }
}

bool TaskManager::IsActive() const
{
    return free_read_.size() < read_tasks_.size() ||
           free_scan_.size() < scan_tasks_.size() || !writing_.empty();
}

void TaskManager::ResumeScheduled()
{
    while (scheduled_.Size() > 0)
    {
        thd_task = scheduled_.Peek();
        scheduled_.Dequeue();
        thd_task->coro_ = thd_task->coro_.resume();
    }
}

void TaskManager::RecycleFinished()
{
    while (finished_.Size() > 0)
    {
        KvTask *task = finished_.Peek();
        finished_.Dequeue();
        FreeTask(task);
    }
}

}  // namespace kvstore