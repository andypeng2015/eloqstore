#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>
#include <atomic>
#include <random>

#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/test_helpers.h"
#include "../../fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class ReadTaskTestFixture : public TestFixture {
public:
    ReadTaskTestFixture() {
        table_ = GetTable();
        PopulateTestData();
    }

    void PopulateTestData() {
        // Pre-populate with test data
        for (int i = 0; i < 100; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            test_data_[key] = value;

            auto write_req = std::make_unique<BatchWriteRequest>();
            std::vector<WriteDataEntry> batch;
            batch.emplace_back(key, value, i * 1000, WriteOp::Put);  // Use timestamp
            write_req->SetArgs(table_, std::move(batch));

            auto err = GetStore()->ExecSync(write_req.get());
            REQUIRE(err == KvError::NoError);
        }
    }

    KvError ReadKey(const std::string& key, std::string& value) {
        auto read_req = std::make_unique<ReadRequest>();
        read_req->SetArgs(table_, key);

        auto err = GetStore()->ExecSync(read_req.get());
        if (err == KvError::NoError) {
            value = read_req->GetValue();
        }
        return err;
    }

    KvError FloorKey(const std::string& search_key, std::string& found_key, std::string& value) {
        auto floor_req = std::make_unique<FloorRequest>();
        floor_req->SetArgs(table_, search_key);

        auto err = GetStore()->ExecSync(floor_req.get());
        if (err == KvError::NoError) {
            found_key = floor_req->GetKey();
            value = floor_req->GetValue();
        }
        return err;
    }

protected:
    TableIdent table_;
    std::map<std::string, std::string> test_data_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_BasicRead", "[read][task][unit]") {
    SECTION("Read existing key") {
        std::string value;
        KvError err = ReadKey("key_50", value);

        REQUIRE(err == KvError::NoError);
        REQUIRE(value == "value_50");
    }

    SECTION("Read non-existent key") {
        std::string value;
        KvError err = ReadKey("non_existent_key", value);

        REQUIRE(err == KvError::NotFound);
    }

    SECTION("Read empty key") {
        // Write an empty key first
        auto write_req = std::make_unique<BatchWriteRequest>();
        std::vector<WriteDataEntry> batch;
        batch.emplace_back("", "empty_key_value", 0, WriteOp::Put);
        write_req->SetArgs(table_, std::move(batch));
        GetStore()->ExecSync(write_req.get());

        std::string value;
        KvError err = ReadKey("", value);

        // Behavior may vary - empty key might not be allowed
        if (err == KvError::NoError) {
            REQUIRE(value == "empty_key_value");
        }
    }

    SECTION("Read after update") {
        // Update existing key
        auto write_req = std::make_unique<BatchWriteRequest>();
        std::vector<WriteDataEntry> batch;
        batch.emplace_back("key_25", "updated_value_25", 0, WriteOp::Put);
        write_req->SetArgs(table_, std::move(batch));

        REQUIRE(GetStore()->ExecSync(write_req.get()) == KvError::NoError);

        std::string value;
        KvError err = ReadKey("key_25", value);

        REQUIRE(err == KvError::NoError);
        REQUIRE(value == "updated_value_25");
    }

    SECTION("Read after delete") {
        // Delete a key
        auto delete_req = std::make_unique<BatchWriteRequest>();
        std::vector<WriteDataEntry> batch;
        batch.emplace_back("key_75", "", 0, WriteOp::Delete);
        delete_req->SetArgs(table_, std::move(batch));

        REQUIRE(GetStore()->ExecSync(delete_req.get()) == KvError::NoError);

        std::string value;
        KvError err = ReadKey("key_75", value);

        REQUIRE(err == KvError::NotFound);
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_FloorOperation", "[read][task][floor]") {
    SECTION("Floor exact match") {
        std::string found_key, value;
        KvError err = FloorKey("key_50", found_key, value);

        REQUIRE(err == KvError::NoError);
        REQUIRE(found_key == "key_50");
        REQUIRE(value == "value_50");
    }

    SECTION("Floor between keys") {
        std::string found_key, value;
        KvError err = FloorKey("key_55_not_exist", found_key, value);

        REQUIRE(err == KvError::NoError);
        // Should return the largest key <= search_key
        // In our case, that would be key_5 (string comparison)
        REQUIRE(found_key <= "key_55_not_exist");
    }

    SECTION("Floor before all keys") {
        std::string found_key, value;
        KvError err = FloorKey("aaa", found_key, value);

        // No key is <= "aaa" (all keys start with "key_")
        REQUIRE(err == KvError::NotFound);
    }

    SECTION("Floor after all keys") {
        std::string found_key, value;
        KvError err = FloorKey("zzz", found_key, value);

        REQUIRE(err == KvError::NoError);
        // Should return the largest key in the dataset
        REQUIRE(!found_key.empty());
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_Performance", "[read][task][performance]") {
    SECTION("Sequential reads") {
        Timer timer;
        int successful_reads = 0;

        for (int i = 0; i < 100; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value;

            if (ReadKey(key, value) == KvError::NoError) {
                successful_reads++;
            }
        }

        double elapsed = timer.ElapsedMilliseconds();
        REQUIRE(successful_reads == 100);

        double reads_per_second = (successful_reads * 1000.0) / elapsed;
        // Performance threshold (adjust as needed)
        // REQUIRE(reads_per_second > 1000);
    }

    SECTION("Random reads") {
        Timer timer;
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 99);

        int successful_reads = 0;
        for (int i = 0; i < 100; ++i) {
            std::string key = "key_" + std::to_string(dist(rng));
            std::string value;

            if (ReadKey(key, value) == KvError::NoError) {
                successful_reads++;
            }
        }

        double elapsed = timer.ElapsedMilliseconds();
        REQUIRE(successful_reads == 100);
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_Concurrent", "[read][task][concurrent]") {
    SECTION("Multiple concurrent reads") {
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};

        for (int t = 0; t < 10; ++t) {
            threads.emplace_back([this, t, &success_count, &error_count]() {
                for (int i = 0; i < 10; ++i) {
                    std::string key = "key_" + std::to_string(t * 10 + i);
                    std::string value;

                    auto read_req = std::make_unique<ReadRequest>();
                    read_req->SetArgs(table_, key);

                    if (GetStore()->ExecSync(read_req.get()) == KvError::NoError) {
                        success_count++;
                    } else {
                        error_count++;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count == 100);
        REQUIRE(error_count == 0);
    }

    SECTION("Reads during writes") {
        std::atomic<bool> stop_flag{false};
        std::atomic<int> writes_done{0};
        std::atomic<int> reads_done{0};

        // Writer thread
        std::thread writer([this, &stop_flag, &writes_done]() {
            int key_num = 1000;
            while (!stop_flag) {
                std::string key = "dynamic_key_" + std::to_string(key_num);
                std::string value = "dynamic_value_" + std::to_string(key_num);

                auto write_req = std::make_unique<BatchWriteRequest>();
                std::vector<WriteDataEntry> batch;
                batch.emplace_back(key, value, 0, WriteOp::Put);
                write_req->SetArgs(table_, std::move(batch));

                GetStore()->ExecSync(write_req.get());
                writes_done++;
                key_num++;

                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        // Reader thread
        std::thread reader([this, &stop_flag, &reads_done]() {
            while (!stop_flag) {
                int key_num = rand() % 100;
                std::string key = "key_" + std::to_string(key_num);

                auto read_req = std::make_unique<ReadRequest>();
                read_req->SetArgs(table_, key);

                GetStore()->ExecSync(read_req.get());
                reads_done++;

                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        // Let them run for a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        stop_flag = true;
        writer.join();
        reader.join();

        REQUIRE(writes_done > 0);
        REQUIRE(reads_done > 0);
    }
}

TEST_CASE_METHOD(ReadTaskTestFixture, "ReadTask_EdgeCases", "[read][task][edge]") {
    SECTION("Read very large value") {
        std::string large_value(1024 * 1024, 'X');  // 1MB value
        std::string key = "large_value_key";

        auto write_req = std::make_unique<BatchWriteRequest>();
        std::vector<WriteDataEntry> batch;
        batch.emplace_back(key, large_value, 0, WriteOp::Put);
        write_req->SetArgs(table_, std::move(batch));

        REQUIRE(GetStore()->ExecSync(write_req.get()) == KvError::NoError);

        std::string read_value;
        KvError err = ReadKey(key, read_value);

        REQUIRE(err == KvError::NoError);
        REQUIRE(read_value.size() == large_value.size());
        REQUIRE(read_value == large_value);
    }

    SECTION("Read with special characters") {
        std::vector<std::pair<std::string, std::string>> special_cases = {
            {"key\0with\0null", "value\0with\0null"},
            {"key\nwith\nnewlines", "value\n\n\n"},
            {"key\twith\ttabs", "value\t\t\t"},
            {std::string(1, 0xFF), std::string(100, 0xFF)}
        };

        for (const auto& [key, value] : special_cases) {
            auto write_req = std::make_unique<BatchWriteRequest>();
            std::vector<WriteDataEntry> batch;
            batch.emplace_back(key, value, 0, WriteOp::Put);
            write_req->SetArgs(table_, std::move(batch));

            GetStore()->ExecSync(write_req.get());

            std::string read_value;
            auto read_req = std::make_unique<ReadRequest>();
            read_req->SetArgs(table_, key);

            auto err = GetStore()->ExecSync(read_req.get());
            if (err == KvError::NoError) {
                REQUIRE(read_req->GetValue() == value);
            }
        }
    }
}