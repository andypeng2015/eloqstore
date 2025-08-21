#include <curl/curl.h>
#include <jsoncpp/json/json.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

enum class ExecutionMode
{
    SYSTEM_CALL,
    HTTP_API
};
enum class OperationType
{
    UPLOAD,
    DOWNLOAD
};

enum class TransferMode
{
    BATCH,      // 整个目录一次性传输
    INDIVIDUAL  // 逐个文件传输
};

class RcloneDaemonClient
{
private:
    std::string daemon_url_;
    CURL *curl_;

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                std::string *userp)
    {
        userp->append((char *) contents, size * nmemb);
        return size * nmemb;
    }

public:
    RcloneDaemonClient(const std::string &url = "http://localhost:5572")
        : daemon_url_(url), curl_(nullptr)
    {
        curl_ = curl_easy_init();
    }

    ~RcloneDaemonClient()
    {
        if (curl_)
        {
            curl_easy_cleanup(curl_);
        }
    }

    bool StartDaemon()
    {
        // 启动rclone daemon
        std::string command =
            "rclone rcd --rc-addr=localhost:5572 --rc-no-auth &";
        int result = std::system(command.c_str());

        // 等待daemon启动
        std::this_thread::sleep_for(std::chrono::seconds(2));

        return CheckDaemonStatus();
    }

    bool CheckDaemonStatus()
    {
        if (!curl_)
            return false;

        std::string response;
        curl_easy_setopt(
            curl_, CURLOPT_URL, (daemon_url_ + "/core/stats").c_str());
        curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 5L);

        CURLcode res = curl_easy_perform(curl_);
        return res == CURLE_OK;
    }

    bool CopyFile(const std::string &srcFs,
                  const std::string &srcRemote,
                  const std::string &dstFs,
                  const std::string &dstRemote,
                  bool verbose = true)
    {
        if (!curl_)
            return false;

        Json::Value request;
        request["srcFs"] = srcFs;
        request["srcRemote"] = srcRemote;
        request["dstFs"] = dstFs;
        request["dstRemote"] = dstRemote;

        if (verbose)
        {
            request["_config"]["LogLevel"] = "INFO";
        }

        // return ExecuteRequest("/operations/copydir", request);
        return ExecuteRequest("/sync/copy", request);
    }

    bool CopyDir(const std::string &srcFs,
                 const std::string &srcRemote,
                 const std::string &dstFs,
                 const std::string &dstRemote,
                 bool verbose = true)
    {
        if (!curl_)
            return false;

        Json::Value request;
        request["srcFs"] = srcFs;
        request["srcRemote"] = srcRemote;
        request["dstFs"] = dstFs;
        request["dstRemote"] = dstRemote;

        if (verbose)
        {
            request["_config"]["LogLevel"] = "INFO";
        }

        return ExecuteRequest("/sync/copy", request);
    }

private:
    bool ExecuteRequest(const std::string &endpoint, const Json::Value &request)
    {
        Json::StreamWriterBuilder builder;
        std::string json_data = Json::writeString(builder, request);

        std::string response;
        struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl_, CURLOPT_URL, (daemon_url_ + endpoint).c_str());
        curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json_data.c_str());
        curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 300L);

        CURLcode res = curl_easy_perform(curl_);
        curl_slist_free_all(headers);

        if (res != CURLE_OK)
        {
            std::cerr << "HTTP请求失败: " << curl_easy_strerror(res)
                      << std::endl;
            return false;
        }

        Json::Value response_json;
        Json::CharReaderBuilder reader_builder;
        std::string errors;
        std::istringstream response_stream(response);

        if (Json::parseFromStream(
                reader_builder, response_stream, &response_json, &errors))
        {
            if (response_json.isMember("error"))
            {
                std::cerr << "rclone错误: " << response_json["error"].asString()
                          << std::endl;
                return false;
            }
            return true;
        }

        return true;
    }
};
class SimpleRcloneTest
{
public:
    SimpleRcloneTest(int num_threads,
                     int tasks_per_thread,
                     const std::string &source_dir,
                     OperationType operation,
                     TransferMode mode,
                     ExecutionMode exec_mode = ExecutionMode::SYSTEM_CALL)
        : num_threads_(num_threads),
          tasks_per_thread_(tasks_per_thread),
          source_dir_(source_dir),
          operation_(operation),
          transfer_mode_(mode),
          execution_mode_(exec_mode),
          total_bytes_(0),
          daemon_client_(nullptr)
    {
        // 创建下载目录
        std::filesystem::create_directories("rclone/data");
        // 如果使用HTTP API模式，初始化daemon客户端
        if (execution_mode_ == ExecutionMode::HTTP_API)
        {
            daemon_client_ = std::make_unique<RcloneDaemonClient>();
            if (!daemon_client_->CheckDaemonStatus())
            {
                std::cout << "启动rclone daemon..." << std::endl;
                if (!daemon_client_->StartDaemon())
                {
                    std::cerr << "启动rclone daemon失败" << std::endl;
                    throw std::runtime_error("Failed to start rclone daemon");
                }
                std::cout << "rclone daemon启动成功" << std::endl;
            }
            else
            {
                std::cout << "rclone daemon已运行" << std::endl;
            }
        }
    }

    void RunTest()
    {
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;

        for (int i = 0; i < 1; ++i)
        {
            threads.emplace_back(&SimpleRcloneTest::WorkerThread, this, i);
        }

        for (auto &t : threads)
        {
            t.join();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        double seconds = duration.count() / 1000.0;
        double mb_total = total_bytes_ / (1024.0 * 1024.0);
        double mb_per_sec = mb_total / seconds;

        std::cout << "测试完成！总耗时: " << duration.count() << " ms ("
                  << seconds << " 秒)" << std::endl;
        std::cout << "总任务数: " << num_threads_ * tasks_per_thread_
                  << std::endl;
        std::cout << "总传输数据: " << std::fixed << std::setprecision(2)
                  << mb_total << " MB" << std::endl;
        std::cout << "吞吐量: " << std::fixed << std::setprecision(2)
                  << mb_per_sec << " MB/s" << std::endl;
        std::cout << "平均每任务耗时: "
                  << duration.count() / (num_threads_ * tasks_per_thread_)
                  << " ms" << std::endl;
    }

private:
    void WorkerThread(int thread_id)
    {
        std::cout << "线程 " << thread_id << " 开始工作 ("
                  << (operation_ == OperationType::UPLOAD ? "上传" : "下载")
                  << ", "
                  << (transfer_mode_ == TransferMode::BATCH ? "批量" : "逐个")
                  << ")" << std::endl;

        for (int task_id = 0; task_id < tasks_per_thread_; ++task_id)
        {
            bool success = false;

            if (operation_ == OperationType::UPLOAD)
            {
                success = PerformUpload(thread_id, task_id);
            }
            else
            {
                success = PerformDownload(thread_id, task_id);
            }

            std::cout << "线程 " << thread_id << " 任务 " << task_id
                      << (success ? " 成功" : " 失败") << std::endl;
        }

        std::cout << "线程 " << thread_id << " 完成工作" << std::endl;
    }
    bool PerformUpload(int thread_id, int task_id)
    {
        if (execution_mode_ == ExecutionMode::HTTP_API)
        {
            return PerformUploadHTTP(thread_id, task_id);
        }
        else
        {
            return PerformUploadSystem(thread_id, task_id);
        }
    }

    bool PerformDownload(int thread_id, int task_id)
    {
        if (execution_mode_ == ExecutionMode::HTTP_API)
        {
            return PerformDownloadHTTP(thread_id, task_id);
        }
        else
        {
            return PerformDownloadSystem(thread_id, task_id);
        }
    }
    bool PerformUploadSystem(int thread_id, int task_id)
    {
        if (transfer_mode_ == TransferMode::BATCH)
        {
            std::string remote_dir = "thread" + std::to_string(thread_id) +
                                     "_task" + std::to_string(task_id);
            std::string command = "rclone copy \"" + source_dir_ +
                                  "/\" minio:db-stress/" + remote_dir + "/ -v";

            int result = std::system(command.c_str());

            if (result == 0)
            {
                try
                {
                    for (const auto &entry :
                         std::filesystem::recursive_directory_iterator(
                             source_dir_))
                    {
                        if (entry.is_regular_file())
                        {
                            total_bytes_ += entry.file_size();
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << "计算目录大小失败: " << e.what() << std::endl;
                }
            }

            return result == 0;
        }
        else
        {
            std::vector<std::string> source_files;
            try
            {
                for (const auto &entry :
                     std::filesystem::directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        source_files.push_back(
                            entry.path().filename().string());
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "读取源目录失败: " << e.what() << std::endl;
                return false;
            }

            if (source_files.empty())
            {
                std::cerr << "源目录中没有文件" << std::endl;
                return false;
            }

            bool all_success = true;
            for (const auto &filename : source_files)
            {
                std::string source_file = source_dir_ + "/" + filename;
                std::string remote_name = "thread" + std::to_string(thread_id) +
                                          "_task" + std::to_string(task_id) +
                                          "_" + filename;

                std::string command = "rclone copyto \"" + source_file +
                                      "\" minio:db-stress/" + remote_name +
                                      " -v";

                int result = std::system(command.c_str());

                if (result == 0)
                {
                    try
                    {
                        total_bytes_ += std::filesystem::file_size(source_file);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "获取文件大小失败: " << e.what()
                                  << std::endl;
                    }
                }
                else
                {
                    all_success = false;
                }
            }

            return all_success;
        }
    }

    bool PerformDownloadSystem(int thread_id, int task_id)
    {
        if (transfer_mode_ == TransferMode::BATCH)
        {
            // 批量下载：整个远程目录一次性下载
            std::string remote_dir = "thread" + std::to_string(thread_id) +
                                     "_task" + std::to_string(task_id);
            std::string local_dir = "rclone/data/" + remote_dir;

            std::string command = "rclone copy minio:db-stress/" + remote_dir +
                                  "/ \"" + local_dir + "/\" -v";

            int result = std::system(command.c_str());

            if (result == 0)
            {
                try
                {
                    for (const auto &entry :
                         std::filesystem::recursive_directory_iterator(
                             local_dir))
                    {
                        if (entry.is_regular_file())
                        {
                            total_bytes_ += entry.file_size();
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << "计算下载大小失败: " << e.what() << std::endl;
                }
            }

            return result == 0;
        }
        else
        {
            std::vector<std::string> source_files;
            try
            {
                for (const auto &entry :
                     std::filesystem::directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        source_files.push_back(
                            entry.path().filename().string());
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "读取源目录失败: " << e.what() << std::endl;
                return false;
            }

            if (source_files.empty())
            {
                std::cerr << "源目录中没有文件" << std::endl;
                return false;
            }

            bool all_success = true;
            for (const auto &filename : source_files)
            {
                std::string remote_name = "thread" + std::to_string(thread_id) +
                                          "_task" + std::to_string(task_id) +
                                          "_" + filename;
                std::string local_file = "rclone/data/" + filename;

                std::string command = "rclone copyto minio:db-stress/" +
                                      remote_name + " \"" + local_file +
                                      "\" -v";

                int result = std::system(command.c_str());

                if (result == 0 && std::filesystem::exists(local_file))
                {
                    try
                    {
                        total_bytes_ += std::filesystem::file_size(local_file);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "获取文件大小失败: " << e.what()
                                  << std::endl;
                    }
                }
                else
                {
                    all_success = false;
                }
            }

            return all_success;
        }
    }
    bool PerformUploadHTTP(int thread_id, int task_id)
    {
        if (!daemon_client_)
            return false;

        if (transfer_mode_ == TransferMode::BATCH)
        {
            std::string remote_dir = "thread" + std::to_string(thread_id) +
                                     "_task" + std::to_string(task_id);

            bool result =
                daemon_client_->CopyDir(source_dir_,  // srcFs: 本地目录
                                        "",  // srcRemote: 空字符串表示整个目录
                                        "minio:db-stress",  // dstFs: 目标存储
                                        remote_dir,  // dstRemote: 远程目录名
                                        true);

            if (result)
            {
                try
                {
                    for (const auto &entry :
                         std::filesystem::recursive_directory_iterator(
                             source_dir_))
                    {
                        if (entry.is_regular_file())
                        {
                            total_bytes_ += entry.file_size();
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << "计算目录大小失败: " << e.what() << std::endl;
                }
            }

            return result;
        }
        else
        {
            // 逐个上传：遍历目录中的每个文件
            std::vector<std::string> source_files;
            try
            {
                for (const auto &entry :
                     std::filesystem::directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        source_files.push_back(
                            entry.path().filename().string());
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "读取目录失败: " << e.what() << std::endl;
                return false;
            }

            bool all_success = true;
            for (const auto &filename : source_files)
            {
                std::string remote_filename =
                    "thread" + std::to_string(thread_id) + "_task" +
                    std::to_string(task_id) + "_" + filename;

                bool result = daemon_client_->CopyFile(
                    source_dir_,        // srcFs: 本地目录
                    filename,           // srcRemote: 文件名
                    "minio:db-stress",  // dstFs: 目标存储
                    remote_filename,    // dstRemote: 远程文件名
                    true);

                if (result)
                {
                    try
                    {
                        std::filesystem::path file_path =
                            std::filesystem::path(source_dir_) / filename;
                        total_bytes_ += std::filesystem::file_size(file_path);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "获取文件大小失败: " << e.what()
                                  << std::endl;
                    }
                }
                else
                {
                    all_success = false;
                }
            }

            return all_success;
        }
    }

    bool PerformDownloadHTTP(int thread_id, int task_id)
    {
        if (!daemon_client_)
            return false;

        if (transfer_mode_ == TransferMode::BATCH)
        {
            // 批量下载：整个远程目录一次性下载
            std::string remote_dir = "thread" + std::to_string(thread_id) +
                                     "_task" + std::to_string(task_id);
            std::string local_dir = "rclone/data/" + remote_dir;

            // 修复：使用 copydir API 而不是 copyfile
            bool result =
                daemon_client_->CopyDir("minio:db-stress",  // srcFs: 远程存储
                                        remote_dir,  // srcRemote: 远程目录名
                                        local_dir,  // dstFs: 本地目录
                                        "",  // dstRemote: 空字符串表示整个目录
                                        true);

            if (result)
            {
                // 计算下载的数据大小
                try
                {
                    for (const auto &entry :
                         std::filesystem::recursive_directory_iterator(
                             local_dir))
                    {
                        if (entry.is_regular_file())
                        {
                            total_bytes_ += entry.file_size();
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << "计算下载大小失败: " << e.what() << std::endl;
                }
            }

            return result;
        }
        else
        {
            // 逐个下载：根据本地目录中的文件名下载对应的远程文件
            std::vector<std::string> source_files;
            try
            {
                for (const auto &entry :
                     std::filesystem::directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        source_files.push_back(
                            entry.path().filename().string());
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "读取源目录失败: " << e.what() << std::endl;
                return false;
            }

            if (source_files.empty())
            {
                std::cerr << "源目录中没有文件" << std::endl;
                return false;
            }

            bool all_success = true;
            for (const auto &filename : source_files)
            {
                std::string remote_name = "thread" + std::to_string(thread_id) +
                                          "_task" + std::to_string(task_id) +
                                          "_" + filename;
                std::string local_file = "rclone/data/" + filename;

                // 修复：使用正确的4个参数调用 CopyFile
                bool result = daemon_client_->CopyFile(
                    "minio:db-stress",  // srcFs: 远程存储
                    remote_name,        // srcRemote: 远程文件名
                    "rclone/data",      // dstFs: 本地目录
                    filename,           // dstRemote: 本地文件名
                    true);

                if (result && std::filesystem::exists(local_file))
                {
                    try
                    {
                        total_bytes_ += std::filesystem::file_size(local_file);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "获取文件大小失败: " << e.what()
                                  << std::endl;
                    }
                }
                else
                {
                    all_success = false;
                }
            }

            return all_success;
        }
    }
    int num_threads_;
    int tasks_per_thread_;
    std::string source_dir_;
    OperationType operation_;
    TransferMode transfer_mode_;
    ExecutionMode execution_mode_;
    std::atomic<uint64_t> total_bytes_;
    std::unique_ptr<RcloneDaemonClient> daemon_client_;
};

int main(int argc, char *argv[])
{
    if (argc < 5)
    {
        std::cout << "用法: " << argv[0]
                  << " <线程数> <每线程任务数> <源目录> <操作类型> [传输模式] "
                     "[执行模式]"
                  << std::endl;
        std::cout << "操作类型: upload 或 download" << std::endl;
        std::cout
            << "传输模式: batch (批量) 或 individual (逐个), 默认为 individual"
            << std::endl;
        std::cout
            << "执行模式: system (system调用) 或 http (HTTP API), 默认为 system"
            << std::endl;
        std::cout << "示例: " << argv[0]
                  << " 2 5 /path/to/dir upload batch http" << std::endl;
        return 1;
    }

    int num_threads = std::atoi(argv[1]);
    int tasks_per_thread = std::atoi(argv[2]);
    std::string source_dir = argv[3];
    std::string operation_str = argv[4];

    OperationType operation;
    if (operation_str == "upload")
    {
        operation = OperationType::UPLOAD;
    }
    else if (operation_str == "download")
    {
        operation = OperationType::DOWNLOAD;
    }
    else
    {
        std::cerr << "错误：操作类型必须是 'upload' 或 'download'" << std::endl;
        return 1;
    }

    TransferMode transfer_mode = TransferMode::INDIVIDUAL;  // 默认逐个传输
    if (argc >= 6)
    {
        std::string mode_str = argv[5];
        if (mode_str == "batch")
        {
            transfer_mode = TransferMode::BATCH;
        }
        else if (mode_str == "individual")
        {
            transfer_mode = TransferMode::INDIVIDUAL;
        }
        else
        {
            std::cerr << "错误：传输模式必须是 'batch' 或 'individual'"
                      << std::endl;
            return 1;
        }
    }

    ExecutionMode execution_mode =
        ExecutionMode::SYSTEM_CALL;  // 默认system调用
    if (argc >= 7)
    {
        std::string exec_str = argv[6];
        if (exec_str == "system")
        {
            execution_mode = ExecutionMode::SYSTEM_CALL;
        }
        else if (exec_str == "http")
        {
            execution_mode = ExecutionMode::HTTP_API;
        }
        else
        {
            std::cerr << "错误：执行模式必须是 'system' 或 'http'" << std::endl;
            return 1;
        }
    }

    std::cout << "开始rclone压力测试..." << std::endl;
    std::cout << "线程数: " << num_threads << std::endl;
    std::cout << "每线程任务数: " << tasks_per_thread << std::endl;
    std::cout << "总任务数: " << num_threads * tasks_per_thread << std::endl;
    std::cout << "源文件目录: " << source_dir << std::endl;
    std::cout << "操作类型: "
              << (operation == OperationType::UPLOAD ? "上传" : "下载")
              << std::endl;
    std::cout << "传输模式: "
              << (transfer_mode == TransferMode::BATCH ? "批量" : "逐个")
              << std::endl;
    std::cout << "执行模式: "
              << (execution_mode == ExecutionMode::SYSTEM_CALL ? "System调用"
                                                               : "HTTP API")
              << std::endl;

    try
    {
        SimpleRcloneTest test(num_threads,
                              tasks_per_thread,
                              source_dir,
                              operation,
                              transfer_mode,
                              execution_mode);
        test.RunTest();
    }
    catch (const std::exception &e)
    {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}