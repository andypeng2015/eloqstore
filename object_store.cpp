#include "object_store.h"

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "task.h"
namespace eloqstore
{
namespace fs = std::filesystem;
ObjectStore::ObjectStore(const KvOptions *options)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    async_http_mgr_ = std::make_unique<AsyncHttpManager>(options);
}

ObjectStore::~ObjectStore()
{
    curl_global_cleanup();
}

void ObjectStore::DownloadTask::SetupHttpRequest(AsyncHttpManager *manager,
                                                 CURL *easy)
{
    Json::Value request;
    request["srcFs"] =
        manager->options_->cloud_store_path + "/" + tbl_id_->ToString();
    request["srcRemote"] = filename_.data();

    fs::path dir_path = tbl_id_->StorePath(manager->options_->store_path);
    request["dstFs"] = dir_path.string();
    request["dstRemote"] = filename_.data();

    Json::StreamWriterBuilder builder;
    json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, manager->daemon_download_url_.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    headers_ = headers;
}

void ObjectStore::UploadTask::SetupHttpRequest(AsyncHttpManager *manager,
                                               CURL *easy)
{
    fs::path dir_path = tbl_id_->StorePath(manager->options_->store_path);

    curl_mime *mime = curl_mime_init(easy);

    for (const std::string &filename : filenames_)
    {
        std::string filepath = (dir_path / filename).string();
        if (std::filesystem::exists(filepath))
        {
            curl_mimepart *part = curl_mime_addpart(mime);
            curl_mime_name(part, "file");
            curl_mime_filedata(part, filepath.c_str());
        }
    }

    std::string fs_param =
        manager->options_->cloud_store_path + "/" + tbl_id_->ToString();
    std::string url = manager->daemon_upload_url_ + fs_param;

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_MIMEPOST, mime);

    mime_ = mime;
}

void ObjectStore::ListTask::SetupHttpRequest(AsyncHttpManager *manager,
                                             CURL *easy)
{
    Json::Value request;
    request["fs"] = manager->options_->cloud_store_path;
    request["remote"] = remote_path_;
    request["opt"] = Json::Value(Json::objectValue);
    request["opt"]["recurse"] = false;
    request["opt"]["showHash"] = false;

    Json::StreamWriterBuilder builder;
    json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, manager->daemon_list_url_.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    headers_ = headers;
}

void ObjectStore::DeleteTask::SetupHttpRequest(AsyncHttpManager *manager,
                                               CURL *easy)
{
    Json::Value request;
    request["fs"] = manager->options_->cloud_store_path;
    request["remote"] = remote_path_;

    Json::StreamWriterBuilder builder;
    json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers_ = headers;

    const char *url = is_dir_ ? manager->daemon_purge_url_.c_str()
                              : manager->daemon_delete_url_.c_str();

    curl_easy_setopt(easy, CURLOPT_URL, url);
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
}

AsyncHttpManager::AsyncHttpManager(const KvOptions *options)
    : daemon_url_(options->cloud_store_daemon_url),
      daemon_upload_url_(daemon_url_ + "/operations/uploadfile?remote=&fs="),
      daemon_download_url_(daemon_url_ + "/operations/copyfile"),
      daemon_list_url_(daemon_url_ + "/operations/list"),
      daemon_delete_url_(daemon_url_ + "/operations/deletefile"),
      daemon_purge_url_(daemon_url_ + "/operations/purge"),
      options_(options)
{
    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }

    // set the max connections
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 200L);
}

AsyncHttpManager::~AsyncHttpManager()
{
    Cleanup();
    if (multi_handle_)
    {
        curl_multi_cleanup(multi_handle_);
    }
}

void AsyncHttpManager::PerformRequests()
{
    ProcessPendingRetries();
    curl_multi_perform(multi_handle_, &running_handles_);
}

void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    bool is_retry = task->waiting_retry_;
    task->waiting_retry_ = false;

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        task->kv_task_->Resume();
        return;
    }

    // base configuration
    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &task->response_data_);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, task);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

    task->SetupHttpRequest(this, easy);

    // add to multi handle
    CURLMcode mres = curl_multi_add_handle(multi_handle_, easy);
    if (mres != CURLM_OK)
    {
        LOG(ERROR) << "Failed to add handle to multi: "
                   << curl_multi_strerror(mres);
        curl_easy_cleanup(easy);
        task->error_ = KvError::CloudErr;
        task->kv_task_->Resume();
        return;
    }

    // record the active request using CURL handle as key
    active_requests_[easy] = task;
    if (!is_retry)
    {
        task->kv_task_->inflight_io_++;
    }
}

void AsyncHttpManager::ProcessCompletedRequests()
{
    if (IsIdle())
    {
        return;
    }

    CURLMsg *msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)))
    {
        if (msg->msg == CURLMSG_DONE)
        {
            CURL *easy = msg->easy_handle;
            ObjectStore::Task *task;
            curl_easy_getinfo(
                easy, CURLINFO_PRIVATE, reinterpret_cast<void **>(&task));

            if (!task)
            {
                LOG(ERROR) << "Task is null in ProcessCompletedRequests";
                curl_multi_remove_handle(multi_handle_, easy);
                curl_easy_cleanup(easy);
                continue;
            }

            int64_t response_code;
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);

            bool schedule_retry = false;
            uint32_t retry_delay_ms = 0;

            if (msg->data.result == CURLE_OK)
            {
                if (response_code >= 200 && response_code < 300)
                {
                    task->error_ = KvError::NoError;
                }
                else if (response_code == 404)
                {
                    task->error_ = KvError::NotFound;
                }
                else if (IsHttpRetryable(response_code) &&
                         task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING) << "HTTP error " << response_code
                                 << " from rclone, scheduling retry "
                                 << unsigned(task->retry_count_) << "/"
                                 << unsigned(task->max_retries_) << " in "
                                 << retry_delay_ms << " ms";
                }
                else
                {
                    LOG(ERROR) << "HTTP error: " << response_code;
                    task->error_ = ClassifyHttpError(response_code);
                }
            }
            else
            {
                if (IsCurlRetryable(msg->data.result) &&
                    task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING)
                        << "cURL transport error: "
                        << curl_easy_strerror(msg->data.result)
                        << ", scheduling retry " << unsigned(task->retry_count_)
                        << "/" << unsigned(task->max_retries_) << " in "
                        << retry_delay_ms << " ms";
                }
                else
                {
                    LOG(ERROR) << "cURL error: "
                               << curl_easy_strerror(msg->data.result);
                    task->error_ = ClassifyCurlError(msg->data.result);
                }
            }

            // clean curl resources first
            curl_multi_remove_handle(multi_handle_, easy);
            curl_easy_cleanup(easy);
            active_requests_.erase(easy);
            CleanupTaskResources(task);

            if (schedule_retry)
            {
                ScheduleRetry(task, std::chrono::milliseconds(retry_delay_ms));
                continue;
            }

            task->kv_task_->FinishIo();
        }
    }
}

void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    curl_slist_free_all(task->headers_);

    if (task->needs_mime_cleanup_)
    {
        auto upload_task = static_cast<ObjectStore::UploadTask *>(task);
        if (upload_task->mime_)
        {
            curl_mime_free(upload_task->mime_);
            upload_task->mime_ = nullptr;
        }
        task->needs_mime_cleanup_ = false;
    }
}

void AsyncHttpManager::Cleanup()
{
    for (auto &[easy, task] : active_requests_)
    {
        // remove easy handle from multi handle
        curl_multi_remove_handle(multi_handle_, easy);

        // clean Task associated resources
        CleanupTaskResources(task);

        // clean cURL easy handle
        curl_easy_cleanup(easy);

        if (task->kv_task_->inflight_io_ > 0)
        {
            task->error_ = KvError::CloudErr;
            task->kv_task_->FinishIo();
        }
    }
}

void AsyncHttpManager::ProcessPendingRetries()
{
    if (pending_retries_.empty())
    {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    auto it = pending_retries_.begin();
    while (it != pending_retries_.end() && it->first <= now)
    {
        ObjectStore::Task *task = it->second;
        it = pending_retries_.erase(it);
        SubmitRequest(task);
    }
}

void AsyncHttpManager::ScheduleRetry(ObjectStore::Task *task,
                                     std::chrono::steady_clock::duration delay)
{
    task->waiting_retry_ = true;
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    for (auto it = pending_retries_.begin(); it != pending_retries_.end();)
    {
        if (it->second == task)
        {
            it = pending_retries_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    auto deadline = std::chrono::steady_clock::now() + delay;
    pending_retries_.emplace(deadline, task);
}

uint32_t AsyncHttpManager::ComputeBackoffMs(uint8_t attempt) const
{
    if (attempt == 0)
    {
        return kInitialRetryDelayMs;
    }

    uint64_t delay = static_cast<uint64_t>(kInitialRetryDelayMs)
                     << (attempt - 1);
    delay = std::min<uint64_t>(delay, kMaxRetryDelayMs);
    return static_cast<uint32_t>(delay);
}

bool AsyncHttpManager::IsCurlRetryable(CURLcode code) const
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_GOT_NOTHING:
    case CURLE_HTTP2_STREAM:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_PARTIAL_FILE:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
        return true;
    default:
        return false;
    }
}

bool AsyncHttpManager::IsHttpRetryable(int64_t response_code) const
{
    switch (response_code)
    {
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
        return true;
    default:
        return false;
    }
}

KvError AsyncHttpManager::ClassifyHttpError(int64_t response_code) const
{
    switch (response_code)
    {
    case 400:
    case 401:
    case 403:
    case 409:
        return KvError::CloudErr;
    case 404:
        return KvError::NotFound;
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
    case 505:
        return KvError::Timeout;
    default:
        return KvError::CloudErr;
    }
}

KvError AsyncHttpManager::ClassifyCurlError(CURLcode code) const
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_GOT_NOTHING:
        return KvError::Timeout;
    default:
        return KvError::CloudErr;
    }
}

}  // namespace eloqstore
