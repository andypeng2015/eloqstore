#include "file_gc.h"

#include <jsoncpp/json/json.h>

#include <boost/algorithm/string/predicate.hpp>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "error.h"
#include "replayer.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift)
{
    for (uint64_t val : tbl)
    {
        if (MappingSnapshot::IsFilePageId(val))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(val);
            FileId file_id = fp_id >> pages_per_file_shift;
            result.emplace(file_id);
        }
    }
};
bool ReadFileContent(fs::path path, std::string &result)
{
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        return false;
    }
    size_t size = fs::file_size(path);
    result.resize(size);
    file.read(result.data(), size);
    return true;
}
void FileGarbageCollector::StartLocalThreadPool(uint16_t n_workers)
{
    assert(workers_.empty());
    workers_.reserve(n_workers);
    for (int i = 0; i < n_workers; i++)
    {
        workers_.emplace_back(&FileGarbageCollector::WorkerRoutine, this);
    }
    LOG(INFO) << "local file garbage collector started";
}

void FileGarbageCollector::Stop()
{
    if (workers_.empty())
    {
        return;
    }
    // Send stop signal to all workers.
    std::vector<GcTask> stop_tasks;
    stop_tasks.resize(workers_.size());
    tasks_.enqueue_bulk(stop_tasks.data(), stop_tasks.size());
    for (auto &w : workers_)
    {
        w.join();
    }
    workers_.clear();
    LOG(INFO) << "local file garbage collector stopped";
}

bool FileGarbageCollector::AddTask(TableIdent tbl_id,
                                   uint64_t ts,
                                   FileId max_file_id,
                                   std::unordered_set<FileId> retained_files)
{
    return tasks_.enqueue(
        GcTask(std::move(tbl_id), ts, max_file_id, std::move(retained_files)));
}

FileGarbageCollector::~FileGarbageCollector()
{
    Stop();
}

void FileGarbageCollector::WorkerRoutine()
{
    while (true)
    {
        GcTask req;
        tasks_.wait_dequeue(req);
        if (req.IsStopSignal())
        {
            break;
        }

        KvError err = ExecuteLocalGC(req);
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "Local GC failed for table " << req.tbl_id_.ToString()
                       << ", error: " << static_cast<int>(err);
        }
    }
}
KvError FileGarbageCollector::ExecuteLocalGC(const GcTask &task)
{
    namespace fs = std::filesystem;

    fs::path dir_path = task.tbl_id_.StorePath(options_->store_path);
    std::vector<uint64_t> archives;
    archives.reserve(options_->num_retained_archives + 1);
    std::vector<FileId> gc_data_files;
    gc_data_files.reserve(128);

    // Scan all archives and data files.
    for (auto &ent : fs::directory_iterator{dir_path})
    {
        const std::string name = ent.path().filename();
        if (boost::algorithm::ends_with(name, TmpSuffix))
        {
            // Skip temporary files.
            continue;
        }
        auto ret = ParseFileName(name);
        if (ret.first == FileNameManifest)
        {
            if (!ret.second.empty())
            {
                uint64_t ts = std::stoull(ret.second.data());
                if (ts <= task.mapping_ts_)
                {
                    archives.emplace_back(ts);
                }
            }
        }
        else if (ret.first == FileNameData)
        {
            FileId file_id = std::stoull(ret.second.data());
            if (file_id < task.max_file_id_)
            {
                gc_data_files.emplace_back(file_id);
            }
        }
    }

    // Clear expired archives
    if (archives.size() > options_->num_retained_archives)
    {
        std::sort(archives.begin(), archives.end(), std::greater<uint64_t>());
        while (archives.size() > options_->num_retained_archives)
        {
            uint64_t ts = archives.back();
            archives.pop_back();
            fs::path path = dir_path;
            path.append(ArchiveName(ts));
            if (fs::remove(path))
            {
                LOG(INFO) << "GC on partition " << dir_path << " removed "
                          << path;
            }
            else
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }

    // Get all currently used data files by archives and manifest.
    Replayer replayer(options_);
    std::string buffer;
    fs::path path = dir_path;
    path.append(FileNameManifest);
    std::unordered_set<FileId> all_retained_files = task.retained_files_;

    for (uint64_t ts : archives)
    {
        path.replace_filename(ArchiveName(ts));
        if (!ReadFileContent(path, buffer))
        {
            return KvError::IoFail;
        }
        MemStoreMgr::Manifest manifest(buffer);
        KvError err = replayer.Replay(&manifest);
        if (err != KvError::NoError)
        {
            if (err == KvError::Corrupted)
            {
                bool ok = fs::remove(path);
                LOG(ERROR) << "found corrupted archive " << path
                           << ", removed=" << ok;
                continue;
            }
            return err;
        }
        GetRetainedFiles(all_retained_files,
                         replayer.mapping_tbl_,
                         options_->pages_per_file_shift);
    }

    // Clear unused data files by any archive.
    for (FileId file_id : gc_data_files)
    {
        if (!all_retained_files.contains(file_id))
        {
            path.replace_filename(DataFileName(file_id));
            if (!fs::remove(path))
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }
    return KvError::NoError;
}
KvError FileGarbageCollector::ExecuteCloudGC(
    const TableIdent &tbl_id,
    uint64_t mapping_ts,
    FileId max_file_id,
    const std::unordered_set<FileId> &retained_files,
    CloudStoreMgr *cloud_mgr)
{
    LOG(INFO) << "Starting cloud GC for table " << tbl_id.ToString()
              << " with mapping_ts " << mapping_ts;

    std::string table_path = tbl_id.ToString();
    std::vector<std::string> cloud_files;
    KvTask *current_task = ThdTask();

    ObjectStore::ListTask list_task(options_,
                                    table_path,
                                    &cloud_files,
                                    [current_task](ObjectStore::Task *task)
                                    { current_task->Resume(); });

    cloud_mgr->GetObjectStore()->GetHttpManager()->SubmitRequest(&list_task);
    current_task->status_ = TaskStatus::Blocked;
    current_task->Yield();

    if (list_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to list cloud files for table " << table_path
                   << ", error: " << static_cast<int>(list_task.error_);
        return list_task.error_;
    }

    // Parse response and get cloud files
    try
    {
        Json::Value response;
        Json::Reader reader;
        if (reader.parse(list_task.response_data_, response))
        {
            if (response.isMember("list") && response["list"].isArray())
            {
                for (const auto &item : response["list"])
                {
                    if (item.isMember("Name") && item["Name"].isString())
                    {
                        cloud_files.push_back(item["Name"].asString());
                    }
                }
            }
        }
        else
        {
            LOG(ERROR) << "Failed to parse JSON response: "
                       << list_task.response_data_;
            return KvError::Corrupted;
        }
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "JSON parsing exception: " << e.what();
        return KvError::Corrupted;
    }

    // Determine files to delete
    std::vector<std::string> files_to_delete;

    for (const std::string &file_name : cloud_files)
    {
        // skip the tmp file
        if (boost::algorithm::ends_with(file_name, TmpSuffix))
        {
            continue;
        }

        auto ret = ParseFileName(file_name);
        bool should_delete = false;

        if (ret.first == FileNameManifest)
        {
            // check whether out of range of number on manifest
            if (!ret.second.empty())
            {
                uint64_t ts = std::stoull(ret.second.data());
                if (ts <= mapping_ts)
                {
                    // TODO:
                    // 暂时跳过manifest文件的删除,和归档文件的解析
                    continue;
                }
            }
        }
        else if (ret.first == FileNameData)
        {
            // check whether the data file in retained_files
            FileId file_id = std::stoull(ret.second.data());
            if (file_id < max_file_id && !retained_files.contains(file_id))
            {
                should_delete = true;
            }
        }

        if (should_delete)
        {
            files_to_delete.push_back(table_path + "/" + file_name);
        }
    }

    if (files_to_delete.empty())
    {
        LOG(INFO) << "No files to delete for table " << table_path;
        return KvError::NoError;
    }

    LOG(INFO) << "Deleting " << files_to_delete.size()
              << " files from cloud storage";

    // Submit all delete tasks using submit + yield pattern
    std::atomic<int> pending_deletes(files_to_delete.size());
    std::atomic<bool> has_error(false);
    KvError delete_error = KvError::NoError;

    // Store delete tasks to manage their lifecycle
    std::vector<std::unique_ptr<ObjectStore::DeleteTask>> delete_tasks;
    delete_tasks.reserve(files_to_delete.size());

    for (const std::string &file_path : files_to_delete)
    {
        auto delete_task = std::make_unique<ObjectStore::DeleteTask>(
            options_,
            file_path,
            [&pending_deletes,
             &has_error,
             &delete_error,
             current_task,
             file_path](ObjectStore::Task *task)
            {
                if (task->error_ != KvError::NoError &&
                    !has_error.exchange(true))
                {
                    LOG(ERROR) << "Failed to delete cloud file: " << file_path
                               << ", error: " << static_cast<int>(task->error_);
                    delete_error = task->error_;
                }

                // If this is the last delete to complete, resume the main task
                if (pending_deletes.fetch_sub(1) == 1)
                {
                    current_task->Resume();
                }

                // Note: task will be automatically cleaned up by unique_ptr
            });

        cloud_mgr->GetObjectStore()->GetHttpManager()->SubmitRequest(
            delete_task.get());
        delete_tasks.push_back(std::move(delete_task));
    }

    // Block and yield until all deletes complete
    current_task->status_ = TaskStatus::Blocked;
    current_task->Yield();

    if (has_error)
    {
        LOG(ERROR) << "Some delete operations failed: "
                   << static_cast<int>(delete_error);
        return delete_error;
    }

    LOG(INFO) << "Successfully deleted " << files_to_delete.size()
              << " files from cloud storage";
    return KvError::NoError;
}
}  // namespace eloqstore