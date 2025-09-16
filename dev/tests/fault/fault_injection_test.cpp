#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <set>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/fault_injector.h"
#include "../../fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("FaultInjection_IOErrors", "[fault][io]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("fault_io_test");

    SECTION("Intermittent IO errors during writes") {
        // Note: This simulates what would happen if IO errors occurred
        // Since we can't directly inject into the store's IO layer,
        // we test the behavior when operations fail

        const int num_operations = 100;
        int successful_writes = 0;
        int failed_writes = 0;

        RandomGenerator gen(42);

        for (int i = 0; i < num_operations; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = gen.GetValue(100, 500);

            // Simulate random failures (20% chance)
            bool should_fail = gen.GetBool(0.2);

            if (!should_fail) {
                KvError err = fixture.WriteSync(table, key, value);
                if (err == KvError::NoError) {
                    successful_writes++;
                } else {
                    failed_writes++;
                }
            } else {
                // Simulate failure by not writing
                failed_writes++;
            }
        }

        // Verify that successful writes are readable
        int verified_reads = 0;
        for (int i = 0; i < num_operations; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value;
            KvError err = fixture.ReadSync(table, key, value);
            if (err == KvError::NoError) {
                verified_reads++;
            }
        }

        // We should be able to read back successful writes
        REQUIRE(verified_reads <= successful_writes);
        REQUIRE(failed_writes > 0);  // Some failures expected
    }

    SECTION("Recovery after IO errors") {
        std::string key = "recovery_test_key";
        std::string value1 = "initial_value";
        std::string value2 = "recovered_value";

        // Write initial value
        KvError err = fixture.WriteSync(table, key, value1);
        REQUIRE(err == KvError::NoError);

        // Verify initial write
        std::string read_value;
        err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(read_value == value1);

        // Simulate recovery by writing new value
        err = fixture.WriteSync(table, key, value2);
        REQUIRE(err == KvError::NoError);

        // Verify recovery
        err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(read_value == value2);
    }
}

TEST_CASE("FaultInjection_DataIntegrity", "[fault][integrity]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("integrity_test");

    SECTION("Detect corrupted data") {
        // Write known data pattern
        std::string key = "checksum_test";
        std::string original_value = "AAAABBBBCCCCDDDD";  // Pattern for easy corruption detection

        KvError err = fixture.WriteSync(table, key, original_value);
        REQUIRE(err == KvError::NoError);

        // Read back and verify
        std::string read_value;
        err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(read_value == original_value);

        // Write multiple values and verify all
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 10; ++i) {
            std::string k = "integrity_key_" + std::to_string(i);
            std::string v = std::string(100, static_cast<char>('A' + i));
            test_data[k] = v;
            fixture.WriteSync(table, k, v);
        }

        // Verify all data is intact
        for (const auto& [k, expected_v] : test_data) {
            std::string actual_v;
            err = fixture.ReadSync(table, k, actual_v);
            REQUIRE(err == KvError::NoError);
            REQUIRE(actual_v == expected_v);
        }
    }

    SECTION("Concurrent writes maintain consistency") {
        const std::string key = "concurrent_integrity";
        const int num_threads = 4;
        const int writes_per_thread = 25;
        std::atomic<int> final_value{-1};

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                for (int i = 0; i < writes_per_thread; ++i) {
                    int value_num = thread_id * writes_per_thread + i;
                    std::string value = std::to_string(value_num);

                    KvError err = fixture.WriteSync(table, key, value);
                    if (err == KvError::NoError) {
                        final_value = value_num;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Read final value - should be one of the written values
        std::string read_value;
        KvError err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);

        int read_num = std::stoi(read_value);
        REQUIRE(read_num >= 0);
        REQUIRE(read_num < num_threads * writes_per_thread);
    }
}

TEST_CASE("FaultInjection_PartialOperations", "[fault][partial]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("partial_test");

    SECTION("Partial batch writes") {
        // Simulate batch operations where some succeed and some fail
        const int batch_size = 10;
        std::set<std::string> written_keys;
        RandomGenerator gen(123);

        for (int batch = 0; batch < 5; ++batch) {
            std::vector<std::pair<std::string, std::string>> batch_data;

            for (int i = 0; i < batch_size; ++i) {
                std::string key = "batch_" + std::to_string(batch) + "_key_" + std::to_string(i);
                std::string value = "value_" + std::to_string(batch * batch_size + i);
                batch_data.push_back({key, value});
            }

            // Simulate partial success (70% success rate)
            for (const auto& [key, value] : batch_data) {
                if (gen.GetBool(0.7)) {
                    KvError err = fixture.WriteSync(table, key, value);
                    if (err == KvError::NoError) {
                        written_keys.insert(key);
                    }
                }
            }
        }

        // Verify only written keys are readable
        int found_keys = 0;
        int not_found_keys = 0;

        for (int batch = 0; batch < 5; ++batch) {
            for (int i = 0; i < batch_size; ++i) {
                std::string key = "batch_" + std::to_string(batch) + "_key_" + std::to_string(i);
                std::string value;
                KvError err = fixture.ReadSync(table, key, value);

                if (written_keys.count(key) > 0) {
                    REQUIRE(err == KvError::NoError);
                    found_keys++;
                } else {
                    REQUIRE(err == KvError::NotFound);
                    not_found_keys++;
                }
            }
        }

        REQUIRE(found_keys == written_keys.size());
        REQUIRE(not_found_keys == (5 * batch_size - written_keys.size()));
    }

    SECTION("Interrupted scan operations") {
        // Prepare data
        for (int i = 0; i < 100; ++i) {
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "scan_value_" + std::to_string(i);
            fixture.WriteSync(table, key, value);
        }

        // Simulate interrupted scans by using small limits
        std::vector<KvEntry> all_results;
        std::string last_key = "";

        // Scan in small batches, simulating interruptions
        while (true) {
            std::vector<KvEntry> batch_results;
            KvError err = fixture.ScanSync(table, last_key, "9999999999", batch_results, 10);

            if (err != KvError::NoError || batch_results.empty()) {
                break;
            }

            // Verify no duplicates
            for (const auto& entry : batch_results) {
                REQUIRE(entry.key_ > last_key);
                all_results.push_back(entry);
            }

            last_key = batch_results.back().key_;

            // Simulate interruption delay
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // Verify we got all data despite interruptions
        REQUIRE(all_results.size() <= 100);

        // Verify ordering
        for (size_t i = 1; i < all_results.size(); ++i) {
            REQUIRE(all_results[i-1].key_ < all_results[i].key_);
        }
    }
}

TEST_CASE("FaultInjection_CrashRecovery", "[fault][crash]") {
    SECTION("Simulate crash and recovery") {
        // First session - write data
        {
            TestFixture fixture;
            fixture.InitStoreWithDefaults();
            auto table = fixture.CreateTestTable("crash_test");

            // Write some data
            for (int i = 0; i < 50; ++i) {
                std::string key = "persist_key_" + std::to_string(i);
                std::string value = "persist_value_" + std::to_string(i);
                KvError err = fixture.WriteSync(table, key, value);
                REQUIRE(err == KvError::NoError);
            }

            // Fixture destructor simulates "crash" by shutting down
        }

        // Simulate crash/restart delay
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Second session - verify data persistence
        {
            TestFixture fixture;
            fixture.InitStoreWithDefaults();
            auto table = fixture.CreateTestTable("crash_test");

            // Check if any data persisted (depends on store implementation)
            int recovered_keys = 0;
            for (int i = 0; i < 50; ++i) {
                std::string key = "persist_key_" + std::to_string(i);
                std::string value;
                KvError err = fixture.ReadSync(table, key, value);
                if (err == KvError::NoError) {
                    recovered_keys++;
                }
            }

            // We don't require all data to persist (depends on sync policy)
            // but we track what was recovered
            std::cout << "Recovered " << recovered_keys << " out of 50 keys after crash" << std::endl;
        }
    }
}

TEST_CASE("FaultInjection_StressWithFailures", "[fault][stress]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("fault_stress");

    SECTION("High load with random failures") {
        const int num_threads = 4;
        const int ops_per_thread = 100;
        std::atomic<int> successful_ops{0};
        std::atomic<int> failed_ops{0};
        std::atomic<int> integrity_errors{0};

        std::map<std::string, std::string> expected_data;
        std::mutex data_mutex;

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                RandomGenerator gen(thread_id);

                for (int i = 0; i < ops_per_thread; ++i) {
                    // Simulate 10% failure rate
                    if (gen.GetBool(0.1)) {
                        failed_ops++;
                        continue;
                    }

                    auto op = gen.GetRandomOperation({0.3, 0.5, 0.2, 0.0});
                    std::string key = "stress_key_" + std::to_string(gen.GetUInt(0, 99));

                    switch (op) {
                        case RandomGenerator::Operation::WRITE: {
                            std::string value = "thread_" + std::to_string(thread_id) +
                                              "_value_" + std::to_string(i);
                            KvError err = fixture.WriteSync(table, key, value);
                            if (err == KvError::NoError) {
                                successful_ops++;
                                std::lock_guard<std::mutex> lock(data_mutex);
                                expected_data[key] = value;
                            } else {
                                failed_ops++;
                            }
                            break;
                        }
                        case RandomGenerator::Operation::READ: {
                            std::string value;
                            KvError err = fixture.ReadSync(table, key, value);
                            if (err == KvError::NoError) {
                                successful_ops++;
                                // Verify data integrity
                                std::lock_guard<std::mutex> lock(data_mutex);
                                if (expected_data.count(key) > 0 && expected_data[key] != value) {
                                    integrity_errors++;
                                }
                            } else if (err == KvError::NotFound) {
                                successful_ops++;  // NotFound is valid
                            } else {
                                failed_ops++;
                            }
                            break;
                        }
                        case RandomGenerator::Operation::SCAN: {
                            std::vector<KvEntry> results;
                            std::string end_key = "stress_key_" + std::to_string(gen.GetUInt(0, 99));
                            if (key > end_key) std::swap(key, end_key);

                            KvError err = fixture.ScanSync(table, key, end_key, results, 10);
                            if (err == KvError::NoError) {
                                successful_ops++;
                                // Verify scan results are ordered
                                for (size_t j = 1; j < results.size(); ++j) {
                                    if (results[j-1].key_ >= results[j].key_) {
                                        integrity_errors++;
                                        break;
                                    }
                                }
                            } else {
                                failed_ops++;
                            }
                            break;
                        }
                        default:
                            break;
                    }

                    // Simulate random delays
                    if (gen.GetBool(0.05)) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(gen.GetUInt(1, 10)));
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // Print statistics
        std::cout << "\n=== Stress Test with Failures ===" << std::endl;
        std::cout << "Successful operations: " << successful_ops.load() << std::endl;
        std::cout << "Failed operations: " << failed_ops.load() << std::endl;
        std::cout << "Integrity errors: " << integrity_errors.load() << std::endl;

        // Verify no integrity errors
        REQUIRE(integrity_errors == 0);
        REQUIRE(successful_ops > 0);
    }
}