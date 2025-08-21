#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

class AWSS3Uploader
{
public:
    AWSS3Uploader(const std::string &endpoint,
                  const std::string &access_key,
                  const std::string &secret_key,
                  const std::string &region = "us-east-1")
        : total_bytes_uploaded_(0), total_files_uploaded_(0)
    {
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;
        config.region = region;

        Aws::Auth::AWSCredentials credentials(access_key, secret_key);

        s3_client = std::make_unique<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false  // useVirtualAddressing
        );
    }

    ~AWSS3Uploader()
    {
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }

    bool uploadFile(const std::string &bucket_name,
                    const std::string &key,
                    const std::string &file_path,
                    bool verbose = true)
    {
        if (!std::filesystem::exists(file_path))
        {
            if (verbose)
                std::cout << "错误：文件不存在 - " << file_path << std::endl;
            return false;
        }

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(key);

        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file.is_open())
        {
            if (verbose)
                std::cout << "无法打开文件: " << file_path << std::endl;
            return false;
        }

        std::streamsize file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        file.close();

        auto start_time = std::chrono::high_resolution_clock::now();

        request.SetContentLength(file_size);
        request.SetContentType("application/octet-stream");

        auto input_data = Aws::MakeShared<Aws::FStream>(
            "PutObjectInputStream",
            file_path.c_str(),
            std::ios_base::in | std::ios_base::binary);
        request.SetBody(input_data);

        auto outcome = s3_client->PutObject(request);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        if (outcome.IsSuccess())
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            total_bytes_uploaded_ += file_size;
            total_files_uploaded_++;

            if (verbose)
            {
                double speed_mbps =
                    (file_size / 1024.0 / 1024.0) / (duration.count() / 1000.0);
                std::cout << "文件上传成功: " << key << " (大小: " << file_size
                          << " bytes, 耗时: " << duration.count() << "ms"
                          << ", 速度: " << std::fixed << std::setprecision(2)
                          << speed_mbps << " MB/s)" << std::endl;
            }
            return true;
        }
        else
        {
            if (verbose)
            {
                std::cout << "文件上传失败: " << outcome.GetError().GetMessage()
                          << std::endl;
            }
            return false;
        }
    }

    // 串行上传多个文件
    void uploadFilesSerial(const std::string &bucket_name,
                           const std::string &base_dir,
                           int file_count)
    {
        std::cout << "\n=== 开始串行上传测试 ===" << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();

        int success_count = 0;
        for (int i = 0; i < file_count; i++)
        {
            std::string filename = "data_" + std::to_string(i);
            std::string file_path = base_dir + "/" + filename;
            std::string object_key = "serial_" + filename;

            if (uploadFile(bucket_name, object_key, file_path, false))
            {
                success_count++;
                std::cout << "[" << (i + 1) << "/" << file_count << "] "
                          << filename << " 上传成功" << std::endl;
            }
            else
            {
                std::cout << "[" << (i + 1) << "/" << file_count << "] "
                          << filename << " 上传失败" << std::endl;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        std::cout << "\n串行上传完成:" << std::endl;
        std::cout << "- 成功: " << success_count << "/" << file_count << " 文件"
                  << std::endl;
        std::cout << "- 总耗时: " << duration.count() << " ms" << std::endl;

        if (duration.count() > 0)
        {
            double throughput =
                (double) success_count / (duration.count() / 1000.0);
            std::cout << "- 吞吐量: " << std::fixed << std::setprecision(2)
                      << throughput << " 文件/秒" << std::endl;
        }
    }

    // 并发上传多个文件
    void uploadFilesConcurrent(const std::string &bucket_name,
                               const std::string &base_dir,
                               int file_count,
                               int thread_count = 4)
    {
        std::cout << "\n=== 开始并发上传测试 (" << thread_count
                  << " 线程) ===" << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<std::future<bool>> futures;
        std::atomic<int> success_count(0);

        // 创建线程池进行并发上传
        for (int i = 0; i < file_count; i++)
        {
            std::string filename = "data_" + std::to_string(i);
            std::string file_path = base_dir + "/" + filename;
            std::string object_key = "concurrent_" + filename;

            auto future = std::async(
                std::launch::async,
                [this,
                 bucket_name,
                 object_key,
                 file_path,
                 &success_count,
                 i,
                 file_count]()
                {
                    bool result = this->uploadFile(
                        bucket_name, object_key, file_path, false);
                    if (result)
                    {
                        success_count++;
                        std::cout
                            << "[" << (i + 1) << "/" << file_count << "] "
                            << std::filesystem::path(file_path)
                                   .filename()
                                   .string()
                            << " 上传成功 (线程: " << std::this_thread::get_id()
                            << ")" << std::endl;
                    }
                    else
                    {
                        std::cout << "[" << (i + 1) << "/" << file_count << "] "
                                  << std::filesystem::path(file_path)
                                         .filename()
                                         .string()
                                  << " 上传失败" << std::endl;
                    }
                    return result;
                });

            futures.push_back(std::move(future));

            // 控制并发数量
            if (futures.size() >= thread_count)
            {
                for (auto &f : futures)
                {
                    f.wait();
                }
                futures.clear();
            }
        }

        // 等待剩余任务完成
        for (auto &f : futures)
        {
            f.wait();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        std::cout << "\n并发上传完成:" << std::endl;
        std::cout << "- 成功: " << success_count.load() << "/" << file_count
                  << " 文件" << std::endl;
        std::cout << "- 总耗时: " << duration.count() << " ms" << std::endl;

        if (duration.count() > 0)
        {
            double throughput =
                (double) success_count.load() / (duration.count() / 1000.0);
            std::cout << "- 吞吐量: " << std::fixed << std::setprecision(2)
                      << throughput << " 文件/秒" << std::endl;
        }
    }

    size_t getTotalBytesUploaded() const
    {
        return total_bytes_uploaded_;
    }

    size_t getTotalFilesUploaded() const
    {
        return total_files_uploaded_;
    }

    void printTotalStats() const
    {
        double total_mb = total_bytes_uploaded_ / 1024.0 / 1024.0;
        std::cout << "\n=== 总体统计 ===" << std::endl;
        std::cout << "总上传文件数: " << total_files_uploaded_ << " 个"
                  << std::endl;
        std::cout << "总上传大小: " << total_bytes_uploaded_ << " bytes ("
                  << std::fixed << std::setprecision(2) << total_mb << " MB)"
                  << std::endl;
        if (total_files_uploaded_ > 0)
        {
            double avg_size = total_bytes_uploaded_ /
                              (double) total_files_uploaded_ / 1024.0 / 1024.0;
            std::cout << "平均文件大小: " << std::fixed << std::setprecision(2)
                      << avg_size << " MB" << std::endl;
        }
    }

private:
    std::unique_ptr<Aws::S3::S3Client> s3_client;
    std::atomic<size_t> total_bytes_uploaded_;
    std::atomic<size_t> total_files_uploaded_;
    std::mutex stats_mutex_;
};

void printUsage(const char *program_name)
{
    std::cout << "用法: " << program_name << " <模式> [参数]" << std::endl;
    std::cout << "模式:" << std::endl;
    std::cout << "  single <文件路径> [对象键名]  - 单文件上传" << std::endl;
    std::cout
        << "  serial <目录路径> [文件数量]   - 串行批量上传 (默认10个文件)"
        << std::endl;
    std::cout << "  concurrent <目录路径> [文件数量] [线程数] - 并发批量上传 "
                 "(默认10个文件，4线程)"
              << std::endl;
    std::cout << "  both <目录路径> [文件数量] [线程数] - 串行和并发对比测试"
              << std::endl;
    std::cout << "\n示例:" << std::endl;
    std::cout << "  " << program_name << " single /path/to/file.txt"
              << std::endl;
    std::cout << "  " << program_name
              << " serial /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
    std::cout << "  " << program_name
              << " concurrent /home/sjh/eloqstore/rclonetest/rclone/data 10 8"
              << std::endl;
    std::cout << "  " << program_name
              << " both /home/sjh/eloqstore/rclonetest/rclone/data 10 4"
              << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printUsage(argv[0]);
        return 1;
    }

    std::string mode = argv[1];
    std::string bucket_name = "db-stress";

    std::cout << "=== AWS S3 多文件上传测试 ===" << std::endl;
    std::cout << "目标桶: " << bucket_name << std::endl;
    std::cout << "MinIO端点: http://localhost:9000" << std::endl;

    AWSS3Uploader uploader("http://localhost:9000", "ROOTNAME", "CHANGEME123");

    if (mode == "single")
    {
        if (argc < 3)
        {
            std::cout << "错误: 单文件模式需要指定文件路径" << std::endl;
            printUsage(argv[0]);
            return 1;
        }

        std::string file_path = argv[2];
        std::string object_key =
            argc >= 4 ? argv[3]
                      : std::filesystem::path(file_path).filename().string();

        std::cout << "模式: 单文件上传" << std::endl;
        std::cout << "文件路径: " << file_path << std::endl;
        std::cout << "对象键名: " << object_key << std::endl;

        if (uploader.uploadFile(bucket_name, object_key, file_path))
        {
            std::cout << "\n单文件上传成功！" << std::endl;
        }
        else
        {
            std::cout << "\n单文件上传失败！" << std::endl;
            return 1;
        }
    }
    else if (mode == "serial")
    {
        if (argc < 3)
        {
            std::cout << "错误: 串行模式需要指定目录路径" << std::endl;
            printUsage(argv[0]);
            return 1;
        }

        std::string base_dir = argv[2];
        int file_count = argc >= 4 ? std::stoi(argv[3]) : 10;

        std::cout << "模式: 串行批量上传" << std::endl;
        std::cout << "目录路径: " << base_dir << std::endl;
        std::cout << "文件数量: " << file_count << std::endl;

        uploader.uploadFilesSerial(bucket_name, base_dir, file_count);
    }
    else if (mode == "concurrent")
    {
        if (argc < 3)
        {
            std::cout << "错误: 并发模式需要指定目录路径" << std::endl;
            printUsage(argv[0]);
            return 1;
        }

        std::string base_dir = argv[2];
        int file_count = argc >= 4 ? std::stoi(argv[3]) : 10;
        int thread_count = argc >= 5 ? std::stoi(argv[4]) : 4;

        std::cout << "模式: 并发批量上传" << std::endl;
        std::cout << "目录路径: " << base_dir << std::endl;
        std::cout << "文件数量: " << file_count << std::endl;
        std::cout << "线程数量: " << thread_count << std::endl;

        uploader.uploadFilesConcurrent(
            bucket_name, base_dir, file_count, thread_count);
    }
    else if (mode == "both")
    {
        if (argc < 3)
        {
            std::cout << "错误: 对比模式需要指定目录路径" << std::endl;
            printUsage(argv[0]);
            return 1;
        }

        std::string base_dir = argv[2];
        int file_count = argc >= 4 ? std::stoi(argv[3]) : 10;
        int thread_count = argc >= 5 ? std::stoi(argv[4]) : 4;

        std::cout << "模式: 串行vs并发对比测试" << std::endl;
        std::cout << "目录路径: " << base_dir << std::endl;
        std::cout << "文件数量: " << file_count << std::endl;
        std::cout << "并发线程数: " << thread_count << std::endl;

        // 先进行串行测试
        uploader.uploadFilesSerial(bucket_name, base_dir, file_count);

        // 再进行并发测试
        uploader.uploadFilesConcurrent(
            bucket_name, base_dir, file_count, thread_count);
    }
    else
    {
        std::cout << "错误: 未知模式 '" << mode << "'" << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    uploader.printTotalStats();
    return 0;
}