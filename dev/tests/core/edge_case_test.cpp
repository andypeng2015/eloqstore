#include <catch2/catch_test_macros.hpp>
#include <limits>
#include <thread>
#include "../fixtures/test_fixtures.h"
#include "../fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("EdgeCase_BoundaryValues", "[edge][boundary]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("boundary_test");

    SECTION("Empty key and value") {
        // Empty key should be rejected
        KvError err = fixture.WriteSync(table, "", "value");
        REQUIRE(err != KvError::NoError);

        // Empty value should be allowed (treated as delete)
        err = fixture.WriteSync(table, "key_empty_val", "");
        REQUIRE(err == KvError::NoError);

        // Verify empty value
        std::string value;
        err = fixture.ReadSync(table, "key_empty_val", value);
        REQUIRE((err == KvError::NotFound || value.empty()));
    }

    SECTION("Maximum length key") {
        // Test with very long key (up to reasonable limit)
        std::string long_key(4096, 'k');  // 4KB key
        std::string value = "test_value";

        KvError err = fixture.WriteSync(table, long_key, value);
        if (err == KvError::NoError) {
            std::string read_value;
            err = fixture.ReadSync(table, long_key, read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value == value);
        }
    }

    SECTION("Maximum length value") {
        // Test with very large value
        std::string key = "large_value_key";
        std::string large_value(1024 * 1024, 'v');  // 1MB value

        KvError err = fixture.WriteSync(table, key, large_value);
        if (err == KvError::NoError) {
            std::string read_value;
            err = fixture.ReadSync(table, key, read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value.size() == large_value.size());
            REQUIRE(read_value == large_value);
        }
    }

    SECTION("Special characters in keys") {
        std::vector<std::string> special_keys = {
            "key with spaces",
            "key\twith\ttabs",
            "key\nwith\nnewlines",
            "key/with/slashes",
            "key\\with\\backslashes",
            "key:with:colons",
            "key;with;semicolons",
            "key'with'quotes",
            "key\"with\"doublequotes",
            "key|with|pipes",
            "key<with>angles",
            "key{with}braces",
            "key[with]brackets",
            "key(with)parens",
            "key!@#$%^&*()",
            "\x01\x02\x03binary\xFF\xFE\xFD"  // Binary data
        };

        for (const auto& key : special_keys) {
            std::string value = "value_for_" + std::to_string(special_keys.size());

            KvError err = fixture.WriteSync(table, key, value);
            if (err == KvError::NoError) {
                std::string read_value;
                err = fixture.ReadSync(table, key, read_value);
                REQUIRE(err == KvError::NoError);
                REQUIRE(read_value == value);
            }
        }
    }

    SECTION("Numeric boundary keys") {
        // Test integer boundaries as keys
        std::vector<std::pair<std::string, std::string>> boundary_data = {
            {"0", "zero"},
            {"-1", "negative_one"},
            {std::to_string(INT_MIN), "int_min"},
            {std::to_string(INT_MAX), "int_max"},
            {std::to_string(LLONG_MIN), "llong_min"},
            {std::to_string(LLONG_MAX), "llong_max"},
            {std::to_string(UINT_MAX), "uint_max"},
            {std::to_string(ULLONG_MAX), "ullong_max"}
        };

        for (const auto& [key, value] : boundary_data) {
            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }

        // Verify all were written
        for (const auto& [key, expected_value] : boundary_data) {
            std::string value;
            KvError err = fixture.ReadSync(table, key, value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(value == expected_value);
        }
    }
}

TEST_CASE("EdgeCase_ScanBoundaries", "[edge][scan]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("scan_boundary");

    // Prepare data
    for (int i = 0; i < 100; ++i) {
        std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
        fixture.WriteSync(table, key, "value_" + std::to_string(i));
    }

    SECTION("Empty range scan") {
        std::vector<KvEntry> results;
        // Start > End should return empty
        KvError err = fixture.ScanSync(table, "9999", "0000", results);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.empty());
    }

    SECTION("Single key range") {
        std::vector<KvEntry> results;
        // Exact key range
        KvError err = fixture.ScanSync(table, "0000000050", "0000000051", results);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.size() == 1);
        if (!results.empty()) {
            REQUIRE(results[0].key_ == "0000000050");
        }
    }

    SECTION("Full table scan") {
        std::vector<KvEntry> results;
        // Use extreme boundaries
        KvError err = fixture.ScanSync(table, "", "\xFF\xFF\xFF\xFF", results, 1000);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.size() == 100);
    }

    SECTION("Zero limit scan") {
        std::vector<KvEntry> results;
        KvError err = fixture.ScanSync(table, "0000000000", "0000000100", results, 0);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.empty());
    }

    SECTION("Scan with non-existent boundaries") {
        std::vector<KvEntry> results;
        // Keys that don't exist but are valid boundaries
        KvError err = fixture.ScanSync(table, "0000000025.5", "0000000075.5", results);
        REQUIRE(err == KvError::NoError);
        REQUIRE(results.size() == 50);  // Should get keys 26-75
    }
}

TEST_CASE("EdgeCase_RapidOperations", "[edge][rapid]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("rapid_test");

    SECTION("Rapid overwrites") {
        const std::string key = "rapid_key";
        const int iterations = 1000;

        for (int i = 0; i < iterations; ++i) {
            std::string value = "value_" + std::to_string(i);
            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);
        }

        // Final value should be the last one
        std::string final_value;
        KvError err = fixture.ReadSync(table, key, final_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(final_value == "value_" + std::to_string(iterations - 1));
    }

    SECTION("Write-delete cycles") {
        const std::string key = "cycle_key";

        for (int i = 0; i < 100; ++i) {
            // Write
            KvError err = fixture.WriteSync(table, key, "value_" + std::to_string(i));
            REQUIRE(err == KvError::NoError);

            // Verify exists
            std::string value;
            err = fixture.ReadSync(table, key, value);
            REQUIRE(err == KvError::NoError);

            // Delete (write empty)
            err = fixture.WriteSync(table, key, "");
            REQUIRE(err == KvError::NoError);

            // Verify deleted
            err = fixture.ReadSync(table, key, value);
            REQUIRE((err == KvError::NotFound || value.empty()));
        }
    }

    SECTION("Interleaved operations") {
        // Rapidly interleave different operations
        RandomGenerator gen(42);  // Fixed seed for reproducibility

        for (int i = 0; i < 500; ++i) {
            std::string key = "key_" + std::to_string(i % 10);  // Reuse keys

            switch (i % 4) {
                case 0: {
                    // Write
                    fixture.WriteSync(table, key, gen.GetValue(10, 100));
                    break;
                }
                case 1: {
                    // Read
                    std::string value;
                    fixture.ReadSync(table, key, value);
                    break;
                }
                case 2: {
                    // Scan around key
                    std::vector<KvEntry> results;
                    std::string start = "key_" + std::to_string(std::max(0, (i % 10) - 2));
                    std::string end = "key_" + std::to_string(std::min(9, (i % 10) + 2));
                    fixture.ScanSync(table, start, end, results);
                    break;
                }
                case 3: {
                    // Delete (write empty)
                    fixture.WriteSync(table, key, "");
                    break;
                }
            }
        }
    }
}

TEST_CASE("EdgeCase_ConcurrentBoundary", "[edge][concurrent]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("concurrent_boundary");

    SECTION("Concurrent writes to same key") {
        const std::string key = "contested_key";
        const int thread_count = 8;
        const int writes_per_thread = 100;
        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};

        std::vector<std::thread> threads;
        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                for (int i = 0; i < writes_per_thread; ++i) {
                    std::string value = "thread_" + std::to_string(thread_id) +
                                       "_write_" + std::to_string(i);
                    KvError err = fixture.WriteSync(table, key, value);
                    if (err == KvError::NoError) {
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

        // All writes should succeed
        REQUIRE(success_count == thread_count * writes_per_thread);
        REQUIRE(error_count == 0);

        // Final value should be from one of the threads
        std::string final_value;
        KvError err = fixture.ReadSync(table, key, final_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(final_value.find("thread_") == 0);
    }

    SECTION("Concurrent scans with overlapping ranges") {
        // Prepare data
        for (int i = 0; i < 100; ++i) {
            std::string key = "scan_key_" + std::string(5 - std::to_string(i).length(), '0') +
                             std::to_string(i);
            fixture.WriteSync(table, key, "value_" + std::to_string(i));
        }

        const int thread_count = 10;
        std::atomic<int> total_scans{0};
        std::atomic<bool> error_found{false};

        std::vector<std::thread> threads;
        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&, thread_id = t]() {
                for (int i = 0; i < 50; ++i) {
                    std::vector<KvEntry> results;
                    int start = (thread_id * 10) % 90;
                    int end = start + 20;

                    std::string start_key = "scan_key_" +
                        std::string(5 - std::to_string(start).length(), '0') + std::to_string(start);
                    std::string end_key = "scan_key_" +
                        std::string(5 - std::to_string(end).length(), '0') + std::to_string(end);

                    KvError err = fixture.ScanSync(table, start_key, end_key, results);
                    if (err == KvError::NoError) {
                        total_scans++;

                        // Verify results are in order
                        for (size_t j = 1; j < results.size(); ++j) {
                            if (results[j-1].key_ >= results[j].key_) {
                                error_found = true;
                                break;
                            }
                        }
                    } else {
                        error_found = true;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(total_scans == thread_count * 50);
        REQUIRE(!error_found);
    }
}

TEST_CASE("EdgeCase_DataPatterns", "[edge][patterns]") {
    TestFixture fixture;
    fixture.InitStoreWithDefaults();
    auto table = fixture.CreateTestTable("pattern_test");

    SECTION("All zeros and all ones") {
        // Test data patterns that might trigger edge cases in encoding
        std::vector<std::pair<std::string, std::string>> patterns = {
            {std::string(100, '\0'), std::string(100, '\0')},  // All null bytes
            {std::string(100, '\xFF'), std::string(100, '\xFF')},  // All 0xFF bytes
            {std::string(100, '0'), std::string(100, '1')},  // All ASCII 0s and 1s
            {"AAAAAAAAAA", "ZZZZZZZZZZ"},  // Repeated characters
        };

        for (const auto& [key, value] : patterns) {
            KvError err = fixture.WriteSync(table, key, value);
            REQUIRE(err == KvError::NoError);

            std::string read_value;
            err = fixture.ReadSync(table, key, read_value);
            REQUIRE(err == KvError::NoError);
            REQUIRE(read_value == value);
        }
    }

    SECTION("Unicode and extended characters") {
        std::vector<std::pair<std::string, std::string>> unicode_data = {
            {"emoji_😀", "value_with_emoji_🎉"},
            {"chinese_你好", "世界"},
            {"japanese_こんにちは", "ワールド"},
            {"arabic_مرحبا", "عالم"},
            {"mixed_ABC_123_你好_😀", "mixed_value_test"}
        };

        for (const auto& [key, value] : unicode_data) {
            KvError err = fixture.WriteSync(table, key, value);
            if (err == KvError::NoError) {
                std::string read_value;
                err = fixture.ReadSync(table, key, read_value);
                REQUIRE(err == KvError::NoError);
                REQUIRE(read_value == value);
            }
        }
    }

    SECTION("Compressible patterns") {
        // Highly compressible data
        std::string key = "compress_test";
        std::string value;

        // Create highly repetitive pattern
        for (int i = 0; i < 1000; ++i) {
            value += "REPEAT_PATTERN_";
        }

        KvError err = fixture.WriteSync(table, key, value);
        REQUIRE(err == KvError::NoError);

        std::string read_value;
        err = fixture.ReadSync(table, key, read_value);
        REQUIRE(err == KvError::NoError);
        REQUIRE(read_value == value);
    }
}