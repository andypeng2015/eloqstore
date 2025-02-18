#pragma once

#include <atomic>
#include <boost/context/pooled_fixedsize_stack.hpp>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "concurrentqueue.h"
#include "error.h"
#include "index_page_manager.h"
#include "task_manager.h"

namespace kvstore
{

class EloqStore;

class KvRequest
{
public:
    KvError ExecSync(EloqStore *store);
    const TableIdent &TableId() const;
    KvError Error() const;
    bool IsDone() const;

    enum class Type : uint8_t
    {
        Read,
        Scan,
        Write
    };
    Type Typ() const;

    struct Read
    {
        // input
        std::string_view key;
        // output
        std::string val;
        uint64_t ts;
    };

    struct Scan
    {
        // input
        std::string_view begin_key;
        std::string_view end_key;
        // output
        std::vector<KvEntry> results;
    };

    struct Write
    {
        // input
        std::vector<WriteDataEntry> entries;
    };

    void Reset();
    Read &SetRead(TableIdent tid, std::string_view key);
    Scan &SetScan(TableIdent tid, std::string_view begin, std::string_view end);
    void SetWrite(TableIdent tid, std::vector<WriteDataEntry> &&entries);

    template <typename T>
    T &Args()
    {
        return std::get<T>(args_);
    }

    uint64_t user_data_;
    std::function<void(KvRequest *)> wake_;

private:
    void Done(KvError err);

    Type typ_;
    TableIdent tbl_id_;
    std::variant<Read, Scan, Write> args_;

    KvError err_{KvError::NoError};
    std::atomic<bool> done_{false};

    friend class Worker;
    friend class KvTask;
};

class Worker
{
public:
    Worker(const EloqStore *global);
    ~Worker();
    KvError Init(int dir_fd);
    void Start();
    void Stop();
    bool AddRequest(KvRequest *req);

private:
    void Loop();
    void HandleReq(KvRequest *req);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        thd_task = task;
        task->coro_ =
            boost::context::callcc(std::allocator_arg,
                                   stack_pool_,
                                   [task, lbd](continuation &&sink)
                                   {
                                       task->main_ = std::move(sink);
                                       KvError err = lbd();
                                       task->req_->Done(err);
                                       task->req_ = nullptr;
                                       task->status_ = TaskStatus::Idle;
                                       task_mgr->finished_.Enqueue(task);
                                       return std::move(task->main_);
                                   });
    }

    const EloqStore *global_;
    moodycamel::ConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
    boost::context::pooled_fixedsize_stack stack_pool_;
};

class EloqStore
{
public:
    EloqStore(KvOptions opts);
    ~EloqStore();
    KvError Start();
    bool SendRequest(KvRequest *req);
    void Stop();
    bool IsStopped() const;

    const KvOptions options_;

private:
    KvError InitDBDir();
    void CloseDBDir();

    int dir_fd_{-1};
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<bool> stopped_;
};
}  // namespace kvstore