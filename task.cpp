#include "task.h"

#include <cassert>

#include "index_page_manager.h"
#include "task_manager.h"

namespace kvstore
{
void KvTask::Yield()
{
    main_ = main_.resume();
}

void KvTask::Resume()
{
    status_ = TaskStatus::Ongoing;
    task_mgr->scheduled_.Enqueue(this);
}

int KvTask::WaitSyncIo()
{
    assert(inflight_io_ > 0);
    status_ = TaskStatus::WaitSyncIo;
    io_res_ = 0;
    io_flags_ = 0;
    Yield();
    return io_res_;
}

int KvTask::WaitAsynIo(bool all)
{
    if (inflight_io_ == 0)
    {
        return 0;
    }
    status_ = all ? TaskStatus::WaitAllAsynIo : TaskStatus::WaitAnyAsynIo;
    asyn_io_err_ = 0;
    Yield();
    assert(inflight_io_ == 0 || !all);
    return asyn_io_err_;
}

void KvTask::FinishIo(bool is_sync_io)
{
    assert(inflight_io_ > 0);
    inflight_io_--;
    switch (status_)
    {
    case TaskStatus::WaitSyncIo:
        if (is_sync_io)
        {
            Resume();
        }
        break;
    case TaskStatus::WaitAnyAsynIo:
        if (!is_sync_io)
        {
            Resume();
        }
        break;
    case TaskStatus::WaitAllAsynIo:
        if (inflight_io_ == 0)
        {
            Resume();
        }
        break;
    default:
        break;
    }
}

AsyncIoManager *IoMgr()
{
    return index_mgr->IoMgr();
}

const KvOptions *Options()
{
    return index_mgr->Options();
}

const Comparator *Comp()
{
    return Options()->comparator_;
}
}  // namespace kvstore