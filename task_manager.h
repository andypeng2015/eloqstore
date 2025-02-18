
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "circular_queue.h"
#include "read_task.h"
#include "scan_task.h"
#include "table_ident.h"
#include "write_task.h"

namespace kvstore
{
using boost::context::continuation;

class TaskManager
{
public:
    WriteTask *GetWriteTask(const TableIdent &tbl_id);
    ReadTask *GetReadTask();
    ScanTask *GetScanTask();
    void FreeTask(KvTask *task);
    bool IsActive() const;
    void ResumeScheduled();
    void RecycleFinished();

    CircularQueue<KvTask *> scheduled_;
    CircularQueue<KvTask *> finished_;

private:
    std::vector<std::unique_ptr<WriteTask>> write_tasks_;
    std::unordered_map<TableIdent, WriteTask *> writing_;
    std::vector<WriteTask *> free_write_;
    std::vector<std::unique_ptr<ReadTask>> read_tasks_;
    std::vector<ReadTask *> free_read_;
    std::vector<std::unique_ptr<ScanTask>> scan_tasks_;
    std::vector<ScanTask *> free_scan_;
};
}  // namespace kvstore