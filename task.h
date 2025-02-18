#pragma once

#include <boost/context/continuation.hpp>
#include <cassert>
#include <cstdint>

#include "comparator.h"

namespace kvstore
{
class KvRequest;
class KvTask;
class IndexPageManager;
class AsyncIoManager;
class KvOptions;
class TaskManager;

inline thread_local IndexPageManager *index_mgr;
inline thread_local TaskManager *task_mgr;
inline thread_local KvTask *thd_task;

AsyncIoManager *IoMgr();
const KvOptions *Options();
const Comparator *Comp();

enum class TaskStatus : uint8_t
{
    Idle = 0,
    Ongoing,
    Blocked,
    WaitSyncIo,
    WaitAnyAsynIo,
    WaitAllAsynIo
};

enum struct TaskType
{
    Read = 0,
    Scan,
    Write,
};

class KvTask
{
public:
    virtual ~KvTask() = default;
    virtual TaskType Type() const = 0;
    void Yield();
    /**
     * @brief Re-schedules the task to run. Note: the resumed task does not run
     * in place.
     *
     */
    void Resume();

    int WaitSyncIo();
    int WaitAsynIo(bool all = true);
    void FinishIo(bool is_sync_io);

    TaskStatus status_{TaskStatus::Idle};

    uint32_t inflight_io_{0};
    int io_res_{0};
    uint32_t io_flags_{0};
    int asyn_io_err_{0};

    KvRequest *req_{nullptr};
    boost::context::continuation main_;
    boost::context::continuation coro_;
    boost::context::stack_context stack_ctx_;
};
}  // namespace kvstore