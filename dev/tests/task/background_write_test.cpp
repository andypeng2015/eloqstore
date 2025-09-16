#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <map>
#include <ctime>
#include <algorithm>

#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/random_generator.h"

using namespace std::chrono_literals;
using namespace eloqstore;

class BackgroundWriteTestFixture : public TestFixture {
public:
    BackgroundWriteTestFixture() : gen_(42) {
        // Initialize with configuration for background writes
        KvOptions opts;
        opts.data_dir = "/mnt/ramdisk/bg_write_test_" + std::to_string(std::time(nullptr));
        opts.sharding_number = 2;
        opts.enable_direct_io = false;
        opts.write_buffer_size = 1024 * 1024; // 1MB buffer
        opts.background_write_interval_ms = 100; // Fast flush for testing

        InitializeStore(opts);
    }

    void WriteDataAsync(const TableIdent& table, const std::map<std::string, std::string>& data) {
        auto req = std::make_unique<BatchWriteRequest>();
        req->SetMaxTaskNum(1);

        for (const auto& [key, value] : data) {
            req->AddWrite(key, value, 0, WriteOp::Put);
        }

        // Use async execution for background writes
        store_->ExecAsyn(req.release(), 0, [](KvRequest* r) {
            delete r;
        });
    }

    bool VerifyDataEventually(const TableIdent& table, const std::string& key,
                              const std::string& expected_value, int max_retries = 50) {
        for (int i = 0; i < max_retries; ++i) {
            auto req = std::make_unique<ReadRequest>();
            req->table = table;
            req->key = key;
            store_->ExecSync(req.get(), 0);

            if (req->Error() == KvError::kOk && req->value_ == expected_value) {
                return true;
            }

            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    size_t CountPersistedEntries(const TableIdent& table) {
        auto req = std::make_unique<ScanRequest>();
        req->table = table;
        req->key = "";
        req->end = "";
        req->SetLimit(UINT32_MAX);
        store_->ExecSync(req.get(), 0);

        if (req->Error() != KvError::kOk) {
            return 0;
        }
        return req->key_values_.size();
    }

protected:
    RandomGenerator gen_;
};

TEST_CASE_METHOD(BackgroundWriteTestFixture, "Background write async persistence", "[task][background]") {
    TableIdent table{"bg_test", 1};

    SECTION("Single async write persistence") {
        std::string key = "async_key_1";
        std::string value = gen_.GetValue(100, 500);

        WriteDataAsync(table, {{key, value}});

        // Verify data is eventually persisted
        REQUIRE(VerifyDataEventually(table, key, value));
    }

    SECTION("Multiple async writes coalescing") {
        std::map<std::string, std::string> batch1;
        std::map<std::string, std::string> batch2;

        // Create two batches of writes
        for (int i = 0; i < 10; ++i) {
            batch1["batch1_key_" + std::to_string(i)] = gen_.GetValue(100, 500);
            batch2["batch2_key_" + std::to_string(i)] = gen_.GetValue(100, 500);
        }

        // Write both batches quickly (should coalesce)
        WriteDataAsync(table, batch1);
        WriteDataAsync(table, batch2);

        // Wait for background flush
        std::this_thread::sleep_for(200ms);

        // Verify all data is persisted
        for (const auto& [key, value] : batch1) {
            REQUIRE(VerifyDataEventually(table, key, value));
        }
        for (const auto& [key, value] : batch2) {
            REQUIRE(VerifyDataEventually(table, key, value));
        }
    }

    SECTION("Write buffering and batch flush") {
        const int num_writes = 100;
        std::map<std::string, std::string> all_data;

        // Perform many small writes
        for (int i = 0; i < num_writes; ++i) {
            std::string key = "buffer_key_" + std::to_string(i);
            std::string value = gen_.GetValue(50, 200);
            all_data[key] = value;

            WriteDataAsync(table, {{key, value}});

            // Small delay between writes
            std::this_thread::sleep_for(1ms);
        }

        // Wait for all background writes to complete
        std::this_thread::sleep_for(500ms);

        // Verify all data is persisted
        size_t persisted = CountPersistedEntries(table);
        REQUIRE(persisted >= all_data.size());
    }

    SECTION("Concurrent background writes") {
        const int num_threads = 4;
        const int writes_per_thread = 25;
        std::atomic<int> write_count{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([this, &table, t, writes_per_thread, &write_count]() {
                RandomGenerator local_gen(1000 + t);

                for (int i = 0; i < writes_per_thread; ++i) {
                    std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                    std::string value = local_gen.GetValue(100, 500);

                    WriteDataAsync(table, {{key, value}});
                    write_count++;

                    std::this_thread::sleep_for(2ms);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Wait for background writes to complete
        std::this_thread::sleep_for(1s);

        // Verify writes were persisted
        size_t persisted = CountPersistedEntries(table);
        REQUIRE(persisted == num_threads * writes_per_thread);
    }
}

TEST_CASE_METHOD(BackgroundWriteTestFixture, "Background write error handling", "[task][background]") {
    TableIdent table{"bg_error_test", 1};

    SECTION("Write during shutdown") {
        std::map<std::string, std::string> data;
        for (int i = 0; i < 10; ++i) {
            data["shutdown_key_" + std::to_string(i)] = gen_.GetValue(100, 500);
        }

        // Start writes
        WriteDataAsync(table, data);

        // Immediately stop the store
        store_->Stop();

        // Restart store
        store_->Start();

        // Some data might be lost, but store should be functional
        auto req = std::make_unique<ReadRequest>();
        req->table = table;
        req->key = "test_after_restart";
        store_->ExecSync(req.get(), 0);

        // Store should be operational
        REQUIRE((req->Error() == KvError::kOk || req->Error() == KvError::kNotFound));
    }

    SECTION("Memory pressure handling") {
        const int num_large_writes = 100;

        for (int i = 0; i < num_large_writes; ++i) {
            std::string key = "large_key_" + std::to_string(i);
            std::string value = gen_.GetValue(10000, 50000); // Large values

            WriteDataAsync(table, {{key, value}});

            // No delay - stress the buffer
        }

        // System should handle memory pressure gracefully
        std::this_thread::sleep_for(2s);

        // Verify some data was persisted (might not be all due to memory limits)
        size_t persisted = CountPersistedEntries(table);
        REQUIRE(persisted > 0);
    }
}

TEST_CASE_METHOD(BackgroundWriteTestFixture, "Background write performance", "[task][background][performance]") {
    TableIdent table{"bg_perf_test", 1};

    SECTION("Write throughput measurement") {
        const int num_writes = 1000;
        std::map<std::string, std::string> data;

        for (int i = 0; i < num_writes; ++i) {
            data["perf_key_" + std::to_string(i)] = gen_.GetValue(100, 500);
        }

        auto start = std::chrono::steady_clock::now();

        // Write all data asynchronously
        WriteDataAsync(table, data);

        // Wait for completion
        while (CountPersistedEntries(table) < data.size()) {
            std::this_thread::sleep_for(10ms);

            // Timeout after 10 seconds
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > 10s) {
                break;
            }
        }

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        size_t persisted = CountPersistedEntries(table);
        double throughput = (persisted * 1000.0) / duration.count();

        // Should achieve reasonable throughput
        REQUIRE(throughput > 100); // At least 100 ops/sec
    }

    SECTION("Write latency distribution") {
        std::vector<double> latencies;
        const int num_samples = 100;

        for (int i = 0; i < num_samples; ++i) {
            std::string key = "latency_key_" + std::to_string(i);
            std::string value = gen_.GetValue(100, 500);

            auto start = std::chrono::steady_clock::now();
            WriteDataAsync(table, {{key, value}});

            // Wait for persistence
            VerifyDataEventually(table, key, value);
            auto end = std::chrono::steady_clock::now();

            auto latency = std::chrono::duration<double, std::milli>(end - start).count();
            latencies.push_back(latency);
        }

        // Calculate percentiles
        std::sort(latencies.begin(), latencies.end());
        double p50 = latencies[latencies.size() / 2];
        double p95 = latencies[latencies.size() * 95 / 100];
        double p99 = latencies[latencies.size() * 99 / 100];

        // Reasonable latency expectations for background writes
        REQUIRE(p50 < 200);  // p50 under 200ms
        REQUIRE(p95 < 500);  // p95 under 500ms
        REQUIRE(p99 < 1000); // p99 under 1s
    }
}