#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <curl/curl.h>
#include <jsoncpp/json/json.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

int Numtest = 0;
// 抽象上传接口
class FileUploader
{
public:
    virtual ~FileUploader() = default;
    virtual bool uploadFile(const std::string &bucket_name,
                            const std::string &key,
                            const std::string &file_path) = 0;
    virtual std::string getEngineType() const = 0;
};

// AWS S3 上传实现
class AWSS3Uploader : public FileUploader
{
private:
    std::unique_ptr<Aws::S3::S3Client> s3_client;

public:
    AWSS3Uploader(const std::string &endpoint,
                  const std::string &access_key,
                  const std::string &secret_key)
    {
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;
        config.region = "us-east-1";

        Aws::Auth::AWSCredentials credentials(access_key, secret_key);
        s3_client = std::make_unique<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
    }

    ~AWSS3Uploader()
    {
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }

    bool uploadFile(const std::string &bucket_name,
                    const std::string &key,
                    const std::string &file_path) override
    {
        if (!std::filesystem::exists(file_path))
        {
            std::cout << "文件不存在: " << file_path << std::endl;
            return false;
        }

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(key);

        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file.is_open())
        {
            std::cout << "无法打开文件: " << file_path << std::endl;
            return false;
        }

        std::streamsize file_size = file.tellg();
        file.close();

        request.SetContentLength(file_size);
        request.SetContentType("application/octet-stream");

        auto input_data = Aws::MakeShared<Aws::FStream>(
            "PutObjectInputStream",
            file_path.c_str(),
            std::ios_base::in | std::ios_base::binary);
        request.SetBody(input_data);

        auto outcome = s3_client->PutObject(request);
        return outcome.IsSuccess();
    }

    std::string getEngineType() const override
    {
        return "AWS";
    }
};

// Rclone HTTP API 上传实现
class RcloneUploader : public FileUploader
{
private:
    std::string daemon_url;
    CURL *curl;

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                std::string *userp)
    {
        userp->append((char *) contents, size * nmemb);
        return size * nmemb;
    }

    bool ExecuteRequest(const std::string &endpoint,
                        const std::string &json_data,
                        std::string &response)
    {
        struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl, CURLOPT_URL, (daemon_url + endpoint).c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);  // 减少超时时间

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(headers);

        if (res != CURLE_OK)
        {
            std::cout << "cURL 错误: " << curl_easy_strerror(res) << std::endl;
            return false;
        }

        // 检查HTTP响应码
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
        if (response_code != 200)
        {
            std::cout << "HTTP 错误: " << response_code
                      << ", 响应: " << response << std::endl;
            return false;
        }

        return true;
    }

public:
    RcloneUploader(const std::string &url = "http://localhost:5572")
        : daemon_url(url), curl(nullptr)
    {
        curl = curl_easy_init();
        // StartDaemon();
    }

    ~RcloneUploader()
    {
        if (curl)
        {
            curl_easy_cleanup(curl);
        }
    }

    bool StartDaemon()
    {
        std::string command =
            "rclone rcd --rc-addr=localhost:5572 --rc-no-auth &";
        std::system(command.c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));
        return true;
    }

    bool uploadFile(const std::string &bucket_name,
                    const std::string &key,
                    const std::string &file_path) override
    {
        if (!std::filesystem::exists(file_path))
        {
            std::cout << "文件不存在: " << file_path << std::endl;
            return false;
        }

        // 获取绝对路径
        std::filesystem::path abs_path = std::filesystem::absolute(file_path);
        std::string parent_dir = abs_path.parent_path().string();
        std::string filename = abs_path.filename().string();

        // 构造请求 JSON
        Json::Value request;
        request["srcFs"] = parent_dir;
        request["srcRemote"] = filename;
        request["dstFs"] = "minio:" + bucket_name;
        request["dstRemote"] = key;

        Json::StreamWriterBuilder builder;
        std::string jsonString = Json::writeString(builder, request);

        std::cout << "上传请求: " << jsonString << std::endl;

        std::string response;
        bool success =
            ExecuteRequest("/operations/copyfile", jsonString, response);

        if (success)
        {
            // std::cout << "上传成功响应: " << response << std::endl;
        }
        else
        {
            // std::cout << "上传失败响应: " << response << std::endl;
        }

        return success;
    }

    std::string getEngineType() const override
    {
        return "Rclone";
    }
};

// 统一的串行上传测试类
class SerialUploadTester
{
private:
    std::unique_ptr<FileUploader> uploader;
    std::string bucket_name;

public:
    SerialUploadTester(std::unique_ptr<FileUploader> uploader,
                       const std::string &bucket)
        : uploader(std::move(uploader)), bucket_name(bucket)
    {
    }

    void uploadDirectory(const std::string &directory_path)
    {
        std::cout << "\n=== 开始 " << uploader->getEngineType()
                  << " 串行上传测试 ===" << std::endl;

        std::vector<std::string> files;
        try
        {
            for (const auto &entry :
                 std::filesystem::directory_iterator(directory_path))
            {
                if (entry.is_regular_file())
                {
                    files.push_back(entry.path().string());
                }
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "读取目录失败: " << e.what() << std::endl;
            return;
        }

        if (files.empty())
        {
            std::cout << "目录中没有文件" << std::endl;
            return;
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        int success_count = 0;
        if (Numtest > 0)
        {
            files.resize(Numtest);
        }
        for (size_t i = 0; i < files.size(); ++i)
        {
            std::filesystem::path file_path(files[i]);
            std::string filename = file_path.filename().string();
            std::string key = uploader->getEngineType() + "_" + filename;

            if (uploader->uploadFile(bucket_name, key, files[i]))
            {
                success_count++;
                std::cout << "[" << (i + 1) << "/" << files.size() << "] "
                          << filename << " 上传成功" << std::endl;
            }
            else
            {
                std::cout << "[" << (i + 1) << "/" << files.size() << "] "
                          << filename << " 上传失败" << std::endl;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        std::cout << "\n"
                  << uploader->getEngineType() << " 串行上传完成:" << std::endl;
        std::cout << "- 成功: " << success_count << "/" << files.size()
                  << " 文件" << std::endl;
        std::cout << "- 总耗时: " << duration.count() << " ms" << std::endl;

        if (duration.count() > 0)
        {
            double throughput =
                (double) success_count / (duration.count() / 1000.0);
            std::cout << "- 吞吐量: " << std::fixed << std::setprecision(2)
                      << throughput << " 文件/秒" << std::endl;
        }
    }
};

void printUsage(const char *program_name)
{
    std::cout << "用法: " << program_name << " <引擎类型> <目录路径>"
              << std::endl;
    std::cout << "引擎类型:" << std::endl;
    std::cout << "  aws     - 使用AWS SDK" << std::endl;
    std::cout << "  rclone  - 使用Rclone HTTP API" << std::endl;
    std::cout << "  both    - 测试两种引擎" << std::endl;
    std::cout << "\n示例:" << std::endl;
    std::cout << "  " << program_name
              << " aws /home/sjh/eloqstore/rclonetest/rclone/data" << std::endl;
    std::cout << "  " << program_name
              << " rclone /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
    std::cout << "  " << program_name
              << " both /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printUsage(argv[0]);
        return 1;
    }
    if (argc == 4)
    {
        Numtest = std::stoi(argv[3]);
    }
    std::string engine_type = argv[1];
    std::string directory_path = argv[2];
    std::string bucket_name = "db-stress";

    std::cout << "=== 文件上传测试 ===" << std::endl;
    std::cout << "目标桶: " << bucket_name << std::endl;
    std::cout << "源目录: " << directory_path << std::endl;

    if (engine_type == "aws")
    {
        auto uploader = std::make_unique<AWSS3Uploader>(
            "http://localhost:9000", "ROOTNAME", "CHANGEME123");
        SerialUploadTester tester(std::move(uploader), bucket_name);
        tester.uploadDirectory(directory_path);
    }
    else if (engine_type == "rclone")
    {
        auto uploader = std::make_unique<RcloneUploader>();
        SerialUploadTester tester(std::move(uploader), bucket_name);
        tester.uploadDirectory(directory_path);
    }
    else if (engine_type == "both")
    {
        // 测试AWS
        {
            auto uploader = std::make_unique<AWSS3Uploader>(
                "http://localhost:9000", "ROOTNAME", "CHANGEME123");
            SerialUploadTester tester(std::move(uploader), bucket_name);
            tester.uploadDirectory(directory_path);
        }

        // 测试Rclone
        {
            auto uploader = std::make_unique<RcloneUploader>();
            SerialUploadTester tester(std::move(uploader), bucket_name);
            tester.uploadDirectory(directory_path);
        }
    }
    else
    {
        std::cout << "错误: 未知引擎类型 '" << engine_type << "'" << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    return 0;
}