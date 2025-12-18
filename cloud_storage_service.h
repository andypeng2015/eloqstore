#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrentqueue/concurrentqueue.h"
#include "object_store.h"

namespace eloqstore
{
class EloqStore;

class CloudStorageService
{
public:
    explicit CloudStorageService(EloqStore *store);
    ~CloudStorageService();

    void Start();
    void Stop();

    void RegisterObjectStore(ObjectStore *store);
    void UnregisterObjectStore(ObjectStore *store);

    void Submit(ObjectStore *store, ObjectStore::Task *task);
    void NotifyTaskFinished(ObjectStore::Task *task);
    // CloudStoreMgr is responsible for request slot accounting.

private:
    struct PendingJob
    {
        ObjectStore *store;
        ObjectStore::Task *task;
    };

    struct ShardQueue
    {
        moodycamel::ConcurrentQueue<PendingJob> jobs;
    };

    void Run();
    bool DrainPendingJobs();
    bool ProcessHttpWork();

    EloqStore *store_;
    std::thread worker_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::atomic<bool> stopping_{true};

    std::vector<ShardQueue> shard_queues_;
    size_t shard_count_{0};
    std::atomic<uint64_t> pending_jobs_{0};
    std::atomic<size_t> next_queue_{0};
    std::mutex stores_mu_;
    std::vector<ObjectStore *> registered_;
};

}  // namespace eloqstore
