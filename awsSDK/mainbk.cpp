#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// 操作类型枚举
enum class OperationType
{
    UPLOAD,
    DOWNLOAD
};

// 传输模式枚举
enum class TransferMode
{
    BATCH,      // 整个目录一次性传输
    INDIVIDUAL  // 逐个文件传输
};

// AWS S3客户端封装类
class AWSS3Client
{
private:
    std::unique_ptr<Aws::S3::S3Client> s3_client_;
    std::string bucket_name_;

public:
    AWSS3Client(const std::string &endpoint,
                const std::string &access_key,
                const std::string &secret_key,
                const std::string &bucket_name)
        : bucket_name_(bucket_name)
    {
        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;

        Aws::Auth::AWSCredentials credentials(access_key, secret_key);

        s3_client_ = std::make_unique<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false,
            Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET);
    }

    bool UploadFile(const std::string &local_path,
                    const std::string &remote_key)
    {
        std::ifstream file(local_path, std::ios::binary | std::ios::ate);
        if (!file.is_open())
        {
            std::cerr << "无法打开文件: " << local_path << std::endl;
            return false;
        }

        std::streamsize file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        auto request_stream = std::make_shared<std::stringstream>();
        request_stream->write(reinterpret_cast<char *>(file.rdbuf()),
                              file_size);
        file.close();

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name_);
        request.SetKey(remote_key);
        request.SetBody(request_stream);
        request.SetContentLength(file_size);
        request.SetContentType("application/octet-stream");

        auto outcome = s3_client_->PutObject(request);
        return outcome.IsSuccess();
    }

    bool DownloadFile(const std::string &remote_key,
                      const std::string &local_path)
    {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucket_name_);
        request.SetKey(remote_key);

        auto outcome = s3_client_->GetObject(request);
        if (!outcome.IsSuccess())
        {
            std::cerr << "下载失败: " << outcome.GetError().GetMessage()
                      << std::endl;
            return false;
        }

        std::ofstream file(local_path, std::ios::binary);
        if (!file.is_open())
        {
            std::cerr << "无法创建文件: " << local_path << std::endl;
            return false;
        }

        auto &body = outcome.GetResult().GetBody();
        file << body.rdbuf();
        file.close();

        return true;
    }

    std::vector<std::string> ListObjects(const std::string &prefix = "")
    {
        std::vector<std::string> objects;

        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(bucket_name_);
        if (!prefix.empty())
        {
            request.SetPrefix(prefix);
        }

        auto outcome = s3_client_->ListObjectsV2(request);
        if (outcome.IsSuccess())
        {
            for (const auto &object : outcome.GetResult().GetContents())
            {
                objects.push_back(object.GetKey());
            }
        }

        return objects;
    }
};

// AWS S3测试类
class SimpleAWSS3Test
{
public:
    SimpleAWSS3Test(int num_threads,
                    int tasks_per_thread,
                    const std::string &source_dir,
                    OperationType operation,
                    TransferMode mode,
                    const std::string &endpoint = "http://localhost:9000",
                    const std::string &access_key = "ROOTNAME",
                    const std::string &secret_key = "CHANGEME123",
                    const std::string &bucket_name = "db-stress")
        : num_threads_(num_threads),
          tasks_per_thread_(tasks_per_thread),
          source_dir_(source_dir),
          operation_(operation),
          transfer_mode_(mode),
          endpoint_(endpoint),
          access_key_(access_key),
          secret_key_(secret_key),
          bucket_name_(bucket_name),
          total_bytes_(0)
    {
        // 创建下载目录
        std::filesystem::create_directories("aws_downloads");
    }

    void RunTest()
    {
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;

        for (int i = 0; i < num_threads_; ++i)
        {
            threads.emplace_back(&SimpleAWSS3Test::WorkerThread, this, i);
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

        // 每个线程创建自己的S3客户端
        AWSS3Client s3_client(
            endpoint_, access_key_, secret_key_, bucket_name_);

        for (int task_id = 0; task_id < tasks_per_thread_; ++task_id)
        {
            bool success = false;

            auto task_start = std::chrono::high_resolution_clock::now();

            if (operation_ == OperationType::UPLOAD)
            {
                success = PerformUpload(s3_client, thread_id, task_id);
            }
            else
            {
                success = PerformDownload(s3_client, thread_id, task_id);
            }

            auto task_end = std::chrono::high_resolution_clock::now();
            auto task_duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    task_end - task_start);

            std::cout << "线程 " << thread_id << " 任务 " << task_id
                      << (success ? " 成功" : " 失败")
                      << " (耗时: " << task_duration.count() << "ms)"
                      << std::endl;
        }

        std::cout << "线程 " << thread_id << " 完成工作" << std::endl;
    }

    bool PerformUpload(AWSS3Client &client, int thread_id, int task_id)
    {
        if (transfer_mode_ == TransferMode::BATCH)
        {
            // 批量上传：上传整个目录
            std::string remote_prefix = "thread" + std::to_string(thread_id) +
                                        "_task" + std::to_string(task_id) + "/";

            bool all_success = true;
            try
            {
                for (const auto &entry :
                     std::filesystem::recursive_directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        std::string relative_path =
                            std::filesystem::relative(entry.path(), source_dir_)
                                .string();
                        std::string remote_key = remote_prefix + relative_path;

                        if (client.UploadFile(entry.path().string(),
                                              remote_key))
                        {
                            total_bytes_ += entry.file_size();
                        }
                        else
                        {
                            all_success = false;
                        }
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "批量上传失败: " << e.what() << std::endl;
                return false;
            }

            return all_success;
        }
        else
        {
            // 逐个上传：上传目录中的每个文件
            std::vector<std::string> source_files;
            try
            {
                for (const auto &entry :
                     std::filesystem::directory_iterator(source_dir_))
                {
                    if (entry.is_regular_file())
                    {
                        source_files.push_back(entry.path().string());
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
            for (const auto &file_path : source_files)
            {
                std::string filename =
                    std::filesystem::path(file_path).filename().string();
                std::string remote_key = "thread" + std::to_string(thread_id) +
                                         "_task" + std::to_string(task_id) +
                                         "_" + filename;

                if (client.UploadFile(file_path, remote_key))
                {
                    try
                    {
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

    bool PerformDownload(AWSS3Client &client, int thread_id, int task_id)
    {
        if (transfer_mode_ == TransferMode::BATCH)
        {
            // 批量下载：下载指定前缀的所有对象
            std::string remote_prefix = "thread" + std::to_string(thread_id) +
                                        "_task" + std::to_string(task_id) + "/";
            std::string local_dir = "aws_downloads/" + remote_prefix;

            std::filesystem::create_directories(local_dir);

            auto objects = client.ListObjects(remote_prefix);
            bool all_success = true;

            for (const auto &object_key : objects)
            {
                std::string relative_path =
                    object_key.substr(remote_prefix.length());
                std::string local_path = local_dir + relative_path;

                // 创建必要的子目录
                std::filesystem::create_directories(
                    std::filesystem::path(local_path).parent_path());

                if (client.DownloadFile(object_key, local_path))
                {
                    try
                    {
                        total_bytes_ += std::filesystem::file_size(local_path);
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
        else
        {
            // 逐个下载：下载单个文件
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

            bool all_success = true;
            for (const auto &filename : source_files)
            {
                std::string remote_key = "thread" + std::to_string(thread_id) +
                                         "_task" + std::to_string(task_id) +
                                         "_" + filename;
                std::string local_path = "aws_downloads/" + filename;

                if (client.DownloadFile(remote_key, local_path))
                {
                    try
                    {
                        total_bytes_ += std::filesystem::file_size(local_path);
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
    std::string endpoint_;
    std::string access_key_;
    std::string secret_key_;
    std::string bucket_name_;
    std::atomic<uint64_t> total_bytes_;
};

int main(int argc, char *argv[])
{
    // 初始化AWS SDK
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    if (argc < 5)
    {
        std::cout << "用法: " << argv[0]
                  << " <线程数> <每线程任务数> <源目录> <操作类型> [传输模式]"
                  << std::endl;
        std::cout << "操作类型: upload 或 download" << std::endl;
        std::cout
            << "传输模式: batch (批量) 或 individual (逐个), 默认为 individual"
            << std::endl;
        std::cout << "示例: " << argv[0]
                  << " 2 5 "
                     "/home/sjh/eloqstore/db_stress/testdata1/db_stress/"
                     "stress_test0.0 upload individual"
                  << std::endl;

        Aws::ShutdownAPI(options);
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
        Aws::ShutdownAPI(options);
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
            Aws::ShutdownAPI(options);
            return 1;
        }
    }

    std::cout << "开始AWS S3压力测试..." << std::endl;
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

    try
    {
        SimpleAWSS3Test test(num_threads,
                             tasks_per_thread,
                             source_dir,
                             operation,
                             transfer_mode);
        test.RunTest();
    }
    catch (const std::exception &e)
    {
        std::cerr << "测试失败: " << e.what() << std::endl;
        Aws::ShutdownAPI(options);
        return 1;
    }

    // 清理AWS SDK
    Aws::ShutdownAPI(options);
    return 0;
}