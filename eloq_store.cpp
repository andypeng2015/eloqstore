#include "eloq_store.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iterator>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "error.h"
#include "index_page_manager.h"

namespace fs = std::filesystem;

namespace kvstore
{
EloqStore::EloqStore(KvOptions opts) : options_(opts), stopped_(true)
{
    assert(!(options_.data_page_size & (page_align - 1)));

    workers_.reserve(options_.num_workers);
    for (size_t i = 0; i < options_.num_workers; i++)
    {
        workers_.emplace_back(std::make_unique<Worker>(this));
    }
}

EloqStore::~EloqStore()
{
    if (!IsStopped())
    {
        Stop();
    }
    else if (dir_fd_ >= 0)
    {
        // This will happen when the Start() is not successful
        CloseDBDir();
    }
}

KvError EloqStore::Start()
{
    LOG(INFO) << "EloqStore is starting...";
    if (!options_.data_path.empty())
    {
        KvError err = InitDBDir();
        CHECK_KV_ERR(err);
    }

    for (auto &w : workers_)
    {
        KvError err = w->Init(dir_fd_);
        CHECK_KV_ERR(err);
    }

    stopped_.store(false, std::memory_order_relaxed);
    for (auto &w : workers_)
    {
        w->Start();
    }
    return KvError::NoError;
}

KvError EloqStore::InitDBDir()
{
    if (fs::exists(options_.data_path))
    {
        if (!fs::is_directory(options_.data_path))
        {
            LOG(ERROR) << "path " << options_.data_path << " is not directory";
            return KvError::BadDir;
        }
        for (auto &ent : fs::directory_iterator{options_.data_path})
        {
            if (!ent.is_directory())
            {
                LOG(ERROR) << "entry " << ent.path() << " is not directory";
                return KvError::BadDir;
            }
            const std::string name = ent.path().filename().string();
            TableIdent tbl_id = TableIdent::FromString(name);
            if (tbl_id.tbl_name_.empty())
            {
                LOG(ERROR) << "unexpected tablespace name " << name;
                return KvError::BadDir;
            }

            fs::path wal_path = ent.path() / IouringMgr::mani_file;
            if (!fs::exists(wal_path))
            {
                LOG(WARNING) << "remove incomplete tablespace " << name;
                fs::remove_all(ent.path());
            }
        }
    }
    else
    {
        fs::create_directories(options_.data_path);
    }
    dir_fd_ = open(options_.data_path.c_str(), IouringMgr::oflags_dir);
    if (dir_fd_ < 0)
    {
        return KvError::IoFail;
    }
    return KvError::NoError;
}

void EloqStore::CloseDBDir()
{
    if (close(dir_fd_) == 0)
    {
        dir_fd_ = -1;
    }
    else
    {
        LOG(ERROR) << "failed to close database directory " << strerror(errno);
    }
}

bool EloqStore::SendRequest(KvRequest *req)
{
    if (stopped_.load(std::memory_order_relaxed))
    {
        return false;
    }

    Worker *worker = workers_[req->TableId().Hash() % workers_.size()].get();
    return worker->AddRequest(req);
}

void EloqStore::Stop()
{
    stopped_.store(true, std::memory_order_relaxed);
    for (auto &w : workers_)
    {
        w->Stop();
    }

    if (dir_fd_ >= 0)
    {
        CloseDBDir();
    }
    LOG(INFO) << "EloqStore is stopped.";
}

bool EloqStore::IsStopped() const
{
    return stopped_.load(std::memory_order_relaxed);
}

Worker::Worker(const EloqStore *global)
    : global_(global),
      io_mgr_(global->options_.data_path.empty()
                  ? static_cast<std::unique_ptr<AsyncIoManager>>(
                        std::make_unique<MemStoreMgr>(&global->options_))
                  : static_cast<std::unique_ptr<AsyncIoManager>>(
                        std::make_unique<IouringMgr>(&global->options_))),
      index_mgr_(io_mgr_.get()),
      stack_pool_(global->options_.coroutine_stack_size)
{
}

Worker::~Worker()
{
    if (thd_.joinable())
    {
        thd_.join();
    }
}

KvError Worker::Init(int dir_fd)
{
    return io_mgr_->Init(dir_fd);
}

void Worker::Loop()
{
    index_mgr = &index_mgr_;
    task_mgr = &task_mgr_;
    while (true)
    {
        KvRequest *reqs[128];
        size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));
        for (size_t i = 0; i < nreqs; i++)
        {
            KvRequest *req = reqs[i];
            HandleReq(req);
        }
        if (nreqs == 0 && !task_mgr_.IsActive())
        {
            if (global_->IsStopped())
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        io_mgr_->Submit();
        io_mgr_->PollComplete();

        task_mgr_.ResumeScheduled();
        task_mgr_.RecycleFinished();
    }
}

void Worker::Start()
{
    thd_ = std::thread(&Worker::Loop, this);
}

void Worker::Stop()
{
    thd_.join();
}

bool Worker::AddRequest(KvRequest *req)
{
    return requests_.enqueue(req);
}

void Worker::HandleReq(KvRequest *req)
{
    switch (req->typ_)
    {
    case KvRequest::Type::Read:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            KvRequest::Read &args =
                std::get<int(KvRequest::Type::Read)>(req->args_);
            std::string_view ret;
            KvError err = task->Read(req->tbl_id_, args.key, ret, args.ts);
            args.val = ret;
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case KvRequest::Type::Scan:
    {
        ScanTask *task = task_mgr_.GetScanTask();
        auto lbd = [task, req]() -> KvError
        {
            KvRequest::Scan &args =
                std::get<int(KvRequest::Type::Scan)>(req->args_);
            return task->Scan(
                req->tbl_id_, args.begin_key, args.end_key, args.results);
        };
        StartTask(task, req, lbd);
        break;
    }
    case KvRequest::Type::Write:
    {
        WriteTask *task = task_mgr_.GetWriteTask(std::move(req->tbl_id_));
        if (!task)
        {
            req->Done(KvError::WriteConflict);
            break;
        }
        auto lbd = [task, req]() -> KvError
        {
            KvRequest::Write &args =
                std::get<int(KvRequest::Type::Write)>(req->args_);
            task->BulkAddData(std::move(args.entries));
            return task->Apply();
        };
        StartTask(task, req, lbd);
        break;
    }
    }
}

KvError KvRequest::ExecSync(EloqStore *store)
{
    wake_ = [](KvRequest *req) { req->done_.notify_one(); };
    store->SendRequest(this);
    done_.wait(false, std::memory_order_acquire);
    return err_;
}

void KvRequest::Reset()
{
    user_data_ = 0;
    wake_ = nullptr;
    err_ = KvError::NoError;
    done_.store(false, std::memory_order_relaxed);
}

KvRequest::Read &KvRequest::SetRead(TableIdent tid, std::string_view key)
{
    Reset();
    typ_ = Type::Read;
    tbl_id_ = std::move(tid);

    Read &args = args_.emplace<int(Type::Read)>();
    args.key = key;
    return args;
}

KvRequest::Scan &KvRequest::SetScan(TableIdent tid,
                                    std::string_view begin,
                                    std::string_view end)
{
    Reset();
    typ_ = Type::Scan;
    tbl_id_ = std::move(tid);

    Scan &args = args_.emplace<int(Type::Scan)>();
    args.begin_key = begin;
    args.end_key = end;
    return args;
}

void KvRequest::SetWrite(TableIdent tid, std::vector<WriteDataEntry> &&entries)
{
    Reset();
    typ_ = Type::Write;
    tbl_id_ = std::move(tid);

    Write &args = args_.emplace<int(Type::Write)>();
    args.entries = std::move(entries);
}

const TableIdent &KvRequest::TableId() const
{
    return tbl_id_;
}

KvError KvRequest::Error() const
{
    return err_;
}

bool KvRequest::IsDone() const
{
    return done_.load(std::memory_order_acquire);
}

KvRequest::Type KvRequest::Typ() const
{
    return typ_;
}

void KvRequest::Done(KvError err)
{
    err_ = err;
    done_.store(true, std::memory_order_release);
    if (wake_)
    {
        wake_(this);
    }
}

}  // namespace kvstore