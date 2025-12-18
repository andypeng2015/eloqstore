#include "cloud_storage_service.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>

#include "eloq_store.h"
#include "shard.h"

namespace eloqstore
{
namespace
{
constexpr std::chrono::milliseconds kIdleWait{10};
}

CloudStorageService::CloudStorageService(EloqStore *store) : store_(store)
{
    shard_count_ = store_->Options().num_threads;
    shard_queues_.resize(shard_count_);
}

CloudStorageService::~CloudStorageService()
{
    Stop();
}

void CloudStorageService::Start()
{
    bool expected = true;
    if (!stopping_.compare_exchange_strong(expected, false))
    {
        return;
    }
    worker_ = std::thread(&CloudStorageService::Run, this);
}

void CloudStorageService::Stop()
{
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true))
    {
        if (worker_.joinable())
        {
            worker_.join();
        }
        return;
    }
    cv_.notify_all();
    if (worker_.joinable())
    {
        worker_.join();
    }
}

void CloudStorageService::RegisterObjectStore(ObjectStore *store)
{
    std::lock_guard lk(stores_mu_);
    registered_.push_back(store);
}

void CloudStorageService::UnregisterObjectStore(ObjectStore *store)
{
    std::lock_guard lk(stores_mu_);
    auto it = std::remove(registered_.begin(), registered_.end(), store);
    registered_.erase(it, registered_.end());
}

void CloudStorageService::Submit(ObjectStore *store, ObjectStore::Task *task)
{
    CHECK(task != nullptr);
    Shard *owner = task->owner_shard_;
    CHECK(owner != nullptr);
    size_t shard_idx = owner->shard_id_;
    CHECK_LT(shard_idx, shard_queues_.size());
    pending_jobs_.fetch_add(1, std::memory_order_relaxed);
    shard_queues_[shard_idx].jobs.enqueue({store, task});
    cv_.notify_one();
}

void CloudStorageService::NotifyTaskFinished(ObjectStore::Task *task)
{
    if (task == nullptr || task->kv_task_ == nullptr ||
        task->owner_shard_ == nullptr)
    {
        return;
    }
    task->owner_shard_->EnqueueCloudReadyTask(task->kv_task_);
}

void CloudStorageService::Run()
{
    while (!stopping_.load(std::memory_order_acquire))
    {
        bool submitted = DrainPendingJobs();
        bool active = ProcessHttpWork();

        if (stopping_.load(std::memory_order_acquire))
        {
            break;
        }

        if (!submitted && !active)
        {
            std::unique_lock lk(mu_);
            cv_.wait_for(lk, kIdleWait, [this]
                         { return stopping_.load(std::memory_order_acquire) ||
                                  pending_jobs_.load(std::memory_order_acquire) >
                                      0; });
        }
    }

    // Final flush to ensure no pending operations left.
    DrainPendingJobs();
    ProcessHttpWork();
}

bool CloudStorageService::DrainPendingJobs()
{
    bool processed = false;
    if (shard_queues_.empty())
    {
        return false;
    }

    size_t start = next_queue_.fetch_add(1, std::memory_order_relaxed);
    start %= shard_queues_.size();
    for (size_t i = 0; i < shard_queues_.size(); ++i)
    {
        size_t idx = (start + i) % shard_queues_.size();
        PendingJob job;
        while (shard_queues_[idx].jobs.try_dequeue(job))
        {
            pending_jobs_.fetch_sub(1, std::memory_order_relaxed);
            job.store->StartHttpRequest(job.task);
            processed = true;
        }
    }
    return processed;
}

bool CloudStorageService::ProcessHttpWork()
{
    bool active = false;
    std::lock_guard lk(stores_mu_);
    for (ObjectStore *store : registered_)
    {
        if (store == nullptr)
        {
            continue;
        }
        store->RunHttpWork();
        active |= !store->HttpWorkIdle();
    }
    return active;
}

}  // namespace eloqstore
