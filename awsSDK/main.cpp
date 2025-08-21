#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>

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

        s3_client = std::make_shared<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false  // useVirtualAddressing
        );

        // 创建TransferManager
        auto executor =
            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
                "TransferManagerExecutor", 25);
        Aws::Transfer::TransferManagerConfiguration transfer_config(
            executor.get());
        transfer_config.s3Client = s3_client;
        transfer_config.bufferSize = 5 * 1024 * 1024;  // 5MB buffer
        transfer_config.transferBufferMaxHeapSize =
            20 * 1024 * 1024;  // 20MB max heap

        transfer_manager =
            Aws::Transfer::TransferManager::Create(transfer_config);
    }

    ~AWSS3Uploader()
    {
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }
    // 在AWSS3Uploader类中添加测试方法
    bool testConnection(const std::string &bucket_name)
    {
        try
        {
            Aws::S3::Model::HeadBucketRequest request;
            request.SetBucket(bucket_name);

            auto outcome = s3_client->HeadBucket(request);
            if (outcome.IsSuccess())
            {
                std::cout << "连接测试成功，桶存在: " << bucket_name
                          << std::endl;
                return true;
            }
            else
            {
                std::cout << "桶不存在或无权限: "
                          << outcome.GetError().GetMessage() << std::endl;
                return false;
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "连接测试失败: " << e.what() << std::endl;
            return false;
        }
    }
    bool uploadFile(const std::string &bucket_name,
                    const std::string &key,
                    const std::string &file_path,
                    bool verbose = true)
    {
        // 添加TransferManager有效性检查
        if (!transfer_manager)
        {
            if (verbose)
                std::cout << "错误：TransferManager未初始化" << std::endl;
            return false;
        }
        if (!std::filesystem::exists(file_path))
        {
            if (verbose)
                std::cout << "错误：文件不存在 - " << file_path << std::endl;
            return false;
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        std::cout << "start upload " << file_path << std::endl;
        // 使用TransferManager上传文件
        auto uploadHandle = transfer_manager->UploadFile(
            file_path,
            bucket_name,
            key,
            "text/plain",  // 或根据文件类型动态设置
            Aws::Map<Aws::String, Aws::String>()  // 可以添加元数据
        );

        if (!uploadHandle)
        {
            if (verbose)
                std::cout << "错误：创建上传句柄失败" << std::endl;
            return false;
        }
        // 等待上传完成
        uploadHandle->WaitUntilFinished();
        std::cout << "upload " << file_path << " to " << bucket_name << "/"
                  << key << std::endl;
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        if (uploadHandle->GetStatus() ==
            Aws::Transfer::TransferStatus::COMPLETED)
        {
            std::ifstream file(file_path, std::ios::binary | std::ios::ate);
            std::streamsize file_size = file.tellg();
            file.close();

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
                auto error = uploadHandle->GetLastError();
                std::cout << "文件上传失败: " << error.GetMessage()
                          << std::endl;
            }
            return false;
        }
    }
    bool downloadFile(const std::string &bucket_name,
                      const std::string &key,
                      const std::string &local_path)
    {
        auto downloadHandle =
            transfer_manager->DownloadFile(bucket_name, key, local_path);
        downloadHandle->WaitUntilFinished();

        return downloadHandle->GetStatus() ==
               Aws::Transfer::TransferStatus::COMPLETED;
    }
    // 使用TransferManager进行批量上传
    void uploadFilesBatch(const std::string &bucket_name,
                          const std::string &base_dir,
                          int file_count,
                          const std::string &prefix = "batch_")
    {
        std::cout << "\n=== 开始TransferManager批量上传测试 ===" << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();

        std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>>
            upload_handles;
        std::atomic<int> success_count(0);

        // 启动所有上传任务（异步）
        for (int i = 0; i < file_count; i++)
        {
            std::string filename = "data_" + std::to_string(i);
            std::string file_path = base_dir + "/" + filename;
            std::string object_key = prefix + filename;

            if (!std::filesystem::exists(file_path))
            {
                std::cout << "[" << (i + 1) << "/" << file_count << "] "
                          << filename << " 文件不存在，跳过" << std::endl;
                continue;
            }

            // 使用TransferManager异步上传
            auto uploadHandle = transfer_manager->UploadFile(
                file_path,
                bucket_name,
                object_key,
                "application/octet-stream",
                Aws::Map<Aws::String, Aws::String>());

            upload_handles.push_back(uploadHandle);
        }

        // 等待所有上传完成并统计结果
        for (size_t i = 0; i < upload_handles.size(); i++)
        {
            auto &handle = upload_handles[i];
            handle->WaitUntilFinished();

            if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED)
            {
                success_count++;
                std::cout << "[" << (i + 1) << "/" << upload_handles.size()
                          << "] "
                          << "上传成功" << std::endl;
            }
            else
            {
                auto error = handle->GetLastError();
                std::cout << "[" << (i + 1) << "/" << upload_handles.size()
                          << "] "
                          << "上传失败: " << error.GetMessage() << std::endl;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        std::cout << "\nTransferManager批量上传完成:" << std::endl;
        std::cout << "- 成功: " << success_count.load() << "/"
                  << upload_handles.size() << " 文件" << std::endl;
        std::cout << "- 总耗时: " << duration.count() << " ms" << std::endl;

        if (duration.count() > 0)
        {
            double throughput =
                (double) success_count.load() / (duration.count() / 1000.0);
            std::cout << "- 吞吐量: " << std::fixed << std::setprecision(2)
                      << throughput << " 文件/秒" << std::endl;
        }
    }

    // ... existing code ...

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
    std::shared_ptr<Aws::S3::S3Client> s3_client;
    std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager;
    std::atomic<size_t> total_bytes_uploaded_;
    std::atomic<size_t> total_files_uploaded_;
    std::mutex stats_mutex_;
};

void printUsage(const char *program_name)
{
    std::cout << "用法: " << program_name << " <模式> [参数]" << std::endl;
    std::cout << "模式:" << std::endl;
    std::cout << "  single <文件路径> [对象键名]  - 单文件上传" << std::endl;
    std::cout << "  batch <目录路径> [文件数量]   - TransferManager批量上传 "
                 "(默认10个文件)"
              << std::endl;
    std::cout << "\n示例:" << std::endl;
    std::cout << "  " << program_name << " single /path/to/file.txt"
              << std::endl;
    std::cout << "  " << program_name
              << " batch /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
    std::cout << "  " << program_name
              << " batch /home/sjh/eloqstore/rclonetest/rclone/data 10"
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

    std::cout << "=== AWS S3 TransferManager上传测试 ===" << std::endl;
    std::cout << "目标桶: " << bucket_name << std::endl;
    std::cout << "MinIO端点: http://localhost:9000" << std::endl;

    AWSS3Uploader uploader("http://localhost:9000", "ROOTNAME", "CHANGEME123");
    if (!uploader.testConnection(bucket_name))
    {
        std::cout << "无法连接到MinIO服务器，请检查服务状态和认证信息"
                  << std::endl;
        return 1;
    }
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
    else if (mode == "batch")
    {
        if (argc < 3)
        {
            std::cout << "错误: 批量模式需要指定目录路径" << std::endl;
            printUsage(argv[0]);
            return 1;
        }

        std::string base_dir = argv[2];
        int file_count = argc >= 4 ? std::stoi(argv[3]) : 10;

        std::cout << "模式: TransferManager批量上传" << std::endl;
        std::cout << "目录路径: " << base_dir << std::endl;
        std::cout << "文件数量: " << file_count << std::endl;

        uploader.uploadFilesBatch(bucket_name, base_dir, file_count);
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