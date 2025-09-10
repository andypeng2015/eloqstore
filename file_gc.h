#pragma once
#include <thread>
#include <unordered_set>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "object_store.h"
#include "types.h"
// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift);
class ObjectStore;
class FileGarbageCollector
{
public:
    struct GcTask
    {
        GcTask() = default;
        GcTask(TableIdent tbl_id,
               uint64_t ts,
               FileId max_file_id,
               std::unordered_set<FileId> retained_files)
            : tbl_id_(std::move(tbl_id)),
              mapping_ts_(ts),
              max_file_id_(max_file_id),
              retained_files_(std::move(retained_files))
        {
        }
        bool IsStopSignal() const
        {
            return !tbl_id_.IsValid();
        }

        TableIdent tbl_id_;
        uint64_t mapping_ts_{0};
        FileId max_file_id_{0};
        std::unordered_set<FileId> retained_files_;
    };

    explicit FileGarbageCollector(const KvOptions *opts) : options_(opts){};
    ~FileGarbageCollector();

    // Local mode methods (using thread pool)
    void StartLocalThreadPool(uint16_t n_workers);
    void Stop();
    bool AddTask(TableIdent tbl_id,
                 uint64_t ts,
                 FileId max_file_id,
                 std::unordered_set<FileId> retained_files);

    // Cloud mode method (coroutine-based)
    KvError ExecuteCloudGC(const TableIdent &tbl_id,
                           uint64_t mapping_ts,
                           FileId max_file_id,
                           const std::unordered_set<FileId> &retained_files,
                           class CloudStoreMgr *cloud_mgr);

private:
    const KvOptions *options_;

    // Local mode implementation
    KvError ExecuteLocalGC(const GcTask &task);
    void WorkerRoutine();

    // Cloud mode implementation
    KvError ProcessManifestFiles(const std::string &table_path,
                                 uint64_t mapping_ts,
                                 std::unordered_set<FileId> &all_retained_files,
                                 ObjectStore *object_store);

    // Thread pool for local mode
    std::vector<std::thread> workers_;
    moodycamel::BlockingConcurrentQueue<GcTask> tasks_;
};

}  // namespace eloqstore