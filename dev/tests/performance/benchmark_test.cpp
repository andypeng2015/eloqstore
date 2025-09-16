#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <iostream>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/test_config.h"
#include "../../fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;
using namespace std::chrono;

class PerformanceBenchmark {
public:
    struct Metrics {
        std::vector<double> latencies;  // in microseconds
        double total_time_ms = 0;
        uint64_t operations = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_read = 0;

        double GetPercentile(double p) const {
            if (latencies.empty()) return 0;
            auto sorted = latencies;
            std::sort(sorted.begin(), sorted.end());
            size_t idx = static_cast<size_t>(sorted.size() * p / 100.0);
            return sorted[std::min(idx, sorted.size() - 1)];
        }

        double GetAverage() const {
            if (latencies.empty()) return 0;
            return std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        }

        double GetThroughput() const {
            if (total_time_ms == 0) return 0;
            return (operations * 1000.0) / total_time_ms;
        }

        double GetBandwidthMBps() const {
            if (total_time_ms == 0) return 0;
            uint64_t total_bytes = bytes_written + bytes_read;
            return (total_bytes / 1024.0 / 1024.0) / (total_time_ms / 1000.0);
        }

        void Print(const std::string& operation) const {
            std::cout << "\n=== " << operation << " Performance ===" << std::endl;
            std::cout << "Operations: " << operations << std::endl;
            std::cout << "Total time: " << std::fixed << std::setprecision(2)
                      << total_time_ms << " ms" << std::endl;
            std::cout << "Throughput: " << std::fixed << std::setprecision(0)
                      << GetThroughput() << " ops/sec" << std::endl;

            if (!latencies.empty()) {
                std::cout << "Latency (μs):" << std::endl;
                std::cout << "  Average: " << std::fixed << std::setprecision(1)
                          << GetAverage() << std::endl;
                std::cout << "  P50: " << GetPercentile(50) << std::endl;
                std::cout << "  P95: " << GetPercentile(95) << std::endl;
                std::cout << "  P99: " << GetPercentile(99) << std::endl;
                std::cout << "  P99.9: " << GetPercentile(99.9) << std::endl;
            }

            if (bytes_written > 0 || bytes_read > 0) {
                std::cout << "Bandwidth: " << std::fixed << std::setprecision(2)
                          << GetBandwidthMBps() << " MB/s" << std::endl;
            }
        }
    };

    PerformanceBenchmark(TestFixture& fixture, const TestConfig& config)
        : fixture_(fixture), config_(config) {
        table_ = fixture_.CreateTestTable("benchmark");
        gen_.SetSeed(config_.GetEffectiveSeed());
    }

    Metrics BenchmarkWrites(size_t num_ops, size_t key_size, size_t value_size) {
        Metrics metrics;

        // Warmup
        for (int i = 0; i < config_.warmup_iterations; ++i) {
            std::string key = gen_.GetAlphaNumeric(key_size);
            std::string value = gen_.GetValue(value_size, value_size);
            fixture_.WriteSync(table_, key, value);
        }

        // Benchmark
        auto total_start = steady_clock::now();

        for (size_t i = 0; i < num_ops; ++i) {
            std::string key = gen_.GetAlphaNumeric(key_size);
            std::string value = gen_.GetValue(value_size, value_size);

            auto start = high_resolution_clock::now();
            KvError err = fixture_.WriteSync(table_, key, value);
            auto end = high_resolution_clock::now();

            if (err == KvError::NoError) {
                auto duration_us = duration_cast<microseconds>(end - start).count();
                metrics.latencies.push_back(static_cast<double>(duration_us));
                metrics.operations++;
                metrics.bytes_written += key.size() + value.size();
            }
        }

        auto total_end = steady_clock::now();
        metrics.total_time_ms = duration_cast<milliseconds>(total_end - total_start).count();

        return metrics;
    }

    Metrics BenchmarkReads(size_t num_ops, size_t key_size, size_t value_size) {
        Metrics metrics;

        // Prepare data
        std::vector<std::string> keys;
        for (size_t i = 0; i < num_ops; ++i) {
            std::string key = gen_.GetAlphaNumeric(key_size);
            std::string value = gen_.GetValue(value_size, value_size);
            fixture_.WriteSync(table_, key, value);
            keys.push_back(key);
        }

        // Warmup
        for (int i = 0; i < std::min(config_.warmup_iterations, static_cast<int>(keys.size())); ++i) {
            std::string value;
            fixture_.ReadSync(table_, keys[i], value);
        }

        // Benchmark
        auto total_start = steady_clock::now();

        for (const auto& key : keys) {
            std::string value;

            auto start = high_resolution_clock::now();
            KvError err = fixture_.ReadSync(table_, key, value);
            auto end = high_resolution_clock::now();

            if (err == KvError::NoError) {
                auto duration_us = duration_cast<microseconds>(end - start).count();
                metrics.latencies.push_back(static_cast<double>(duration_us));
                metrics.operations++;
                metrics.bytes_read += key.size() + value.size();
            }
        }

        auto total_end = steady_clock::now();
        metrics.total_time_ms = duration_cast<milliseconds>(total_end - total_start).count();

        return metrics;
    }

    Metrics BenchmarkScans(size_t num_ops, size_t range_size) {
        Metrics metrics;

        // Prepare sequential data for scanning
        for (size_t i = 0; i < 10000; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = gen_.GetValue(100, 100);
            fixture_.WriteSync(table_, key, value);
        }

        // Benchmark
        auto total_start = steady_clock::now();

        for (size_t i = 0; i < num_ops; ++i) {
            size_t start_idx = gen_.GetUInt(0, 10000 - range_size);
            std::string start_key = std::string(10 - std::to_string(start_idx).length(), '0') +
                                   std::to_string(start_idx);
            std::string end_key = std::string(10 - std::to_string(start_idx + range_size).length(), '0') +
                                 std::to_string(start_idx + range_size);

            std::vector<KvEntry> results;

            auto start = high_resolution_clock::now();
            KvError err = fixture_.ScanSync(table_, start_key, end_key, results, range_size);
            auto end = high_resolution_clock::now();

            if (err == KvError::NoError) {
                auto duration_us = duration_cast<microseconds>(end - start).count();
                metrics.latencies.push_back(static_cast<double>(duration_us));
                metrics.operations++;

                for (const auto& entry : results) {
                    metrics.bytes_read += entry.key_.size() + entry.value_.size();
                }
            }
        }

        auto total_end = steady_clock::now();
        metrics.total_time_ms = duration_cast<milliseconds>(total_end - total_start).count();

        return metrics;
    }

    Metrics BenchmarkMixedWorkload(size_t num_ops) {
        Metrics metrics;

        // Prepare some data
        std::vector<std::string> existing_keys;
        for (size_t i = 0; i < 1000; ++i) {
            std::string key = gen_.GetAlphaNumeric(32);
            std::string value = gen_.GetValue(100, 500);
            fixture_.WriteSync(table_, key, value);
            existing_keys.push_back(key);
        }

        // Mixed workload: 50% reads, 30% writes, 20% scans
        auto total_start = steady_clock::now();

        for (size_t i = 0; i < num_ops; ++i) {
            auto op = gen_.GetRandomOperation({0.5, 0.3, 0.2, 0.0});

            auto start = high_resolution_clock::now();
            KvError err = KvError::NoError;

            switch (op) {
                case RandomGenerator::Operation::READ: {
                    if (!existing_keys.empty()) {
                        std::string key = existing_keys[gen_.GetUInt(0, existing_keys.size() - 1)];
                        std::string value;
                        err = fixture_.ReadSync(table_, key, value);
                        if (err == KvError::NoError) {
                            metrics.bytes_read += key.size() + value.size();
                        }
                    }
                    break;
                }
                case RandomGenerator::Operation::WRITE: {
                    std::string key = gen_.GetAlphaNumeric(32);
                    std::string value = gen_.GetValue(100, 500);
                    err = fixture_.WriteSync(table_, key, value);
                    if (err == KvError::NoError) {
                        metrics.bytes_written += key.size() + value.size();
                        existing_keys.push_back(key);
                    }
                    break;
                }
                case RandomGenerator::Operation::SCAN: {
                    std::vector<KvEntry> results;
                    std::string start_key = gen_.GetAlphaNumeric(32);
                    std::string end_key = gen_.GetAlphaNumeric(32);
                    if (start_key > end_key) std::swap(start_key, end_key);

                    err = fixture_.ScanSync(table_, start_key, end_key, results, 10);
                    if (err == KvError::NoError) {
                        for (const auto& entry : results) {
                            metrics.bytes_read += entry.key_.size() + entry.value_.size();
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            auto end = high_resolution_clock::now();

            if (err == KvError::NoError) {
                auto duration_us = duration_cast<microseconds>(end - start).count();
                metrics.latencies.push_back(static_cast<double>(duration_us));
                metrics.operations++;
            }
        }

        auto total_end = steady_clock::now();
        metrics.total_time_ms = duration_cast<milliseconds>(total_end - total_start).count();

        return metrics;
    }

private:
    TestFixture& fixture_;
    TestConfig config_;
    TableIdent table_;
    RandomGenerator gen_;
};

TEST_CASE("PerformanceBenchmark_Sequential", "[benchmark][performance]") {
    TestConfig config = TestConfig::LoadConfig();
    TestFixture fixture;
    fixture.InitStoreWithDefaults();

    PerformanceBenchmark benchmark(fixture, config);

    SECTION("Write performance - small values") {
        auto metrics = benchmark.BenchmarkWrites(1000, 32, 100);
        if (config.verbose) {
            metrics.Print("Write (32B key, 100B value)");
        }

        // Basic performance requirements
        REQUIRE(metrics.GetThroughput() > 1000);  // At least 1000 ops/sec
        REQUIRE(metrics.GetPercentile(99) < 10000);  // P99 < 10ms
    }

    SECTION("Write performance - large values") {
        auto metrics = benchmark.BenchmarkWrites(100, 32, 10000);
        if (config.verbose) {
            metrics.Print("Write (32B key, 10KB value)");
        }

        REQUIRE(metrics.operations == 100);
    }

    SECTION("Read performance") {
        auto metrics = benchmark.BenchmarkReads(1000, 32, 100);
        if (config.verbose) {
            metrics.Print("Read (32B key, 100B value)");
        }

        // Reads should be faster than writes
        REQUIRE(metrics.GetThroughput() > 2000);  // At least 2000 ops/sec
        REQUIRE(metrics.GetPercentile(99) < 5000);  // P99 < 5ms
    }

    SECTION("Scan performance") {
        auto metrics = benchmark.BenchmarkScans(100, 100);  // 100 scans, 100 keys each
        if (config.verbose) {
            metrics.Print("Scan (100 keys per scan)");
        }

        REQUIRE(metrics.operations == 100);
    }

    SECTION("Mixed workload") {
        auto metrics = benchmark.BenchmarkMixedWorkload(1000);
        if (config.verbose) {
            metrics.Print("Mixed Workload (50% read, 30% write, 20% scan)");
        }

        REQUIRE(metrics.operations > 900);  // Allow for some failures
    }
}

TEST_CASE("PerformanceBenchmark_Concurrent", "[benchmark][concurrent]") {
    TestConfig config = TestConfig::LoadConfig();
    TestFixture fixture;
    fixture.InitStoreWithDefaults();

    auto table = fixture.CreateTestTable("concurrent_benchmark");

    SECTION("Concurrent write throughput") {
        const int thread_count = 4;
        const int ops_per_thread = 250;
        std::atomic<uint64_t> total_operations{0};
        std::atomic<uint64_t> total_latency_us{0};

        auto start_time = steady_clock::now();

        std::vector<std::thread> threads;
        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                RandomGenerator gen(config.GetEffectiveSeed() + thread_id);

                for (int i = 0; i < ops_per_thread; ++i) {
                    std::string key = "t" + std::to_string(thread_id) + "_" +
                                     gen.GetAlphaNumeric(28);
                    std::string value = gen.GetValue(100, 100);

                    auto op_start = high_resolution_clock::now();
                    KvError err = fixture.WriteSync(table, key, value);
                    auto op_end = high_resolution_clock::now();

                    if (err == KvError::NoError) {
                        total_operations++;
                        auto duration_us = duration_cast<microseconds>(op_end - op_start).count();
                        total_latency_us += duration_us;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end_time = steady_clock::now();
        auto total_time_ms = duration_cast<milliseconds>(end_time - start_time).count();

        if (config.verbose) {
            std::cout << "\n=== Concurrent Write Performance ===" << std::endl;
            std::cout << "Threads: " << thread_count << std::endl;
            std::cout << "Total operations: " << total_operations.load() << std::endl;
            std::cout << "Total time: " << total_time_ms << " ms" << std::endl;
            std::cout << "Aggregate throughput: "
                      << (total_operations.load() * 1000.0 / total_time_ms) << " ops/sec" << std::endl;
            std::cout << "Average latency: "
                      << (total_latency_us.load() / std::max(1UL, total_operations.load()))
                      << " μs" << std::endl;
        }

        REQUIRE(total_operations >= thread_count * ops_per_thread * 0.95);  // Allow 5% failure
    }
}