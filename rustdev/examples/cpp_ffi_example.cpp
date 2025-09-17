/**
 * C++ FFI Example for EloqStore
 *
 * This example demonstrates how to use EloqStore from C++ through the FFI interface.
 * It shows the same operations as the native Rust example for comparison.
 *
 * To compile:
 *   g++ -std=c++17 cpp_ffi_example.cpp -L../target/release -leloqstore -lpthread -ldl -o cpp_example
 *
 * To run:
 *   LD_LIBRARY_PATH=../target/release ./cpp_example
 */

#include <iostream>
#include <cstring>
#include <vector>
#include <chrono>
#include <memory>
#include <cassert>

// Include the FFI header
extern "C" {
#include "../include/eloqstore.h"
}

// RAII wrapper for EloqStore handle
class EloqStoreWrapper {
public:
    EloqStoreWrapper(const char* data_dir) {
        handle = eloqstore_new(data_dir);
        if (!handle) {
            throw std::runtime_error("Failed to create EloqStore");
        }
    }

    ~EloqStoreWrapper() {
        if (handle) {
            eloqstore_free(handle);
        }
    }

    EloqStoreHandle* get() { return handle; }

private:
    EloqStoreHandle* handle;
};

// Helper to measure time
class Timer {
public:
    Timer() : start(std::chrono::high_resolution_clock::now()) {}

    double elapsed_ms() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        return duration.count() / 1000.0;
    }

private:
    std::chrono::high_resolution_clock::time_point start;
};

int main() {
    std::cout << "=== EloqStore C++ FFI Example ===\n\n";

    try {
        // 1. Initialize store
        std::cout << "1. Initializing EloqStore with data directory: ./cpp_example_data\n";
        EloqStoreWrapper store("./cpp_example_data");

        // Start the store
        if (eloqstore_start(store.get()) != 0) {
            throw std::runtime_error("Failed to start store");
        }
        std::cout << "   Store started successfully!\n\n";

        const char* table = "users";

        // 2. Single write operation
        std::cout << "2. Writing single key-value pair:\n";
        {
            const char* key = "user:2001";
            const char* value = R"({"name":"Bob","age":25})";

            Timer timer;
            int result = eloqstore_write(
                store.get(),
                table, strlen(table),
                key, strlen(key),
                (const uint8_t*)value, strlen(value)
            );

            if (result != 0) {
                std::cerr << "   Write failed with error: " << result << "\n";
            } else {
                std::cout << "   Written: " << key << " -> " << value << "\n";
                std::cout << "   Write latency: " << timer.elapsed_ms() << "ms\n";
            }
        }
        std::cout << "\n";

        // 3. Read operation
        std::cout << "3. Reading the key back:\n";
        {
            const char* key = "user:2001";
            uint8_t value_buf[1024];
            size_t value_len = sizeof(value_buf);

            Timer timer;
            int result = eloqstore_read(
                store.get(),
                table, strlen(table),
                key, strlen(key),
                value_buf, &value_len
            );

            std::cout << "   Read latency: " << timer.elapsed_ms() << "ms\n";

            if (result == 0) {
                std::string value(reinterpret_cast<char*>(value_buf), value_len);
                std::cout << "   Value found: " << value << "\n";
            } else if (result == 1) {
                std::cout << "   Key not found!\n";
            } else {
                std::cerr << "   Read failed with error: " << result << "\n";
            }
        }
        std::cout << "\n";

        // 4. Batch write operation
        std::cout << "4. Batch writing multiple users:\n";
        {
            Timer timer;
            int written = 0;

            for (int i = 2002; i <= 2010; i++) {
                std::string key = "user:" + std::to_string(i);
                std::string value = R"({"name":"User)" + std::to_string(i) +
                                  R"(","age":)" + std::to_string(20 + (i % 50)) + "}";

                int result = eloqstore_write(
                    store.get(),
                    table, strlen(table),
                    key.c_str(), key.length(),
                    (const uint8_t*)value.c_str(), value.length()
                );

                if (result == 0) {
                    written++;
                }
            }

            std::cout << "   Written " << written << " users\n";
            std::cout << "   Total time: " << timer.elapsed_ms() << "ms\n";
        }
        std::cout << "\n";

        // 5. Scan operation
        std::cout << "5. Scanning users in range [user:2001, user:2005]:\n";
        {
            const size_t max_entries = 10;
            EloqStoreScanEntry entries[max_entries];
            size_t num_entries = max_entries;

            const char* start_key = "user:2001";
            const char* end_key = "user:2005";

            Timer timer;
            int result = eloqstore_scan(
                store.get(),
                table, strlen(table),
                start_key, strlen(start_key),
                end_key, strlen(end_key),
                entries, &num_entries,
                false  // not reverse
            );

            std::cout << "   Scan latency: " << timer.elapsed_ms() << "ms\n";

            if (result == 0) {
                std::cout << "   Found " << num_entries << " entries:\n";
                for (size_t i = 0; i < num_entries; i++) {
                    std::string key(reinterpret_cast<const char*>(entries[i].key),
                                  entries[i].key_len);
                    std::string value(reinterpret_cast<const char*>(entries[i].value),
                                    entries[i].value_len);
                    std::cout << "     " << key << " -> " << value << "\n";
                }
            } else {
                std::cerr << "   Scan failed with error: " << result << "\n";
            }
        }
        std::cout << "\n";

        // 6. Update operation
        std::cout << "6. Updating user:2001:\n";
        {
            const char* key = "user:2001";
            const char* new_value = R"({"name":"Bob Smith","age":26})";

            int result = eloqstore_write(
                store.get(),
                table, strlen(table),
                key, strlen(key),
                (const uint8_t*)new_value, strlen(new_value)
            );

            if (result == 0) {
                std::cout << "   Updated " << key << " with new data\n";
            } else {
                std::cerr << "   Update failed with error: " << result << "\n";
            }
        }
        std::cout << "\n";

        // 7. Delete operation
        std::cout << "7. Deleting user:2010:\n";
        {
            const char* key = "user:2010";

            int result = eloqstore_delete(
                store.get(),
                table, strlen(table),
                key, strlen(key)
            );

            if (result == 0) {
                std::cout << "   Deleted " << key << "\n";
            } else {
                std::cerr << "   Delete failed with error: " << result << "\n";
            }
        }
        std::cout << "\n";

        // 8. Verify deletion
        std::cout << "8. Verifying deletion:\n";
        {
            const char* key = "user:2010";
            uint8_t value_buf[1024];
            size_t value_len = sizeof(value_buf);

            int result = eloqstore_read(
                store.get(),
                table, strlen(table),
                key, strlen(key),
                value_buf, &value_len
            );

            if (result == 1) {
                std::cout << "   Confirmed: Key has been deleted\n";
            } else if (result == 0) {
                std::cout << "   ERROR: Key still exists!\n";
            } else {
                std::cerr << "   Read failed with error: " << result << "\n";
            }
        }
        std::cout << "\n";

        // 9. Performance test
        std::cout << "9. Performance test - 1000 random reads:\n";
        {
            Timer total_timer;
            int found = 0;
            int not_found = 0;

            for (int i = 0; i < 1000; i++) {
                int key_id = 2001 + (i % 10);
                std::string key = "user:" + std::to_string(key_id);

                uint8_t value_buf[1024];
                size_t value_len = sizeof(value_buf);

                int result = eloqstore_read(
                    store.get(),
                    table, strlen(table),
                    key.c_str(), key.length(),
                    value_buf, &value_len
                );

                if (result == 0) {
                    found++;
                } else if (result == 1) {
                    not_found++;
                }
            }

            double total_ms = total_timer.elapsed_ms();
            std::cout << "   Total reads: 1000\n";
            std::cout << "   Found: " << found << ", Not found: " << not_found << "\n";
            std::cout << "   Total time: " << total_ms << "ms\n";
            std::cout << "   Average latency: " << total_ms / 1000.0 << "ms\n";
            std::cout << "   Throughput: " << 1000.0 / (total_ms / 1000.0) << " ops/sec\n";
        }
        std::cout << "\n";

        // 10. Shutdown
        std::cout << "10. Shutting down store...\n";
        if (eloqstore_stop(store.get()) == 0) {
            std::cout << "    Store stopped successfully!\n";
        } else {
            std::cerr << "    Failed to stop store\n";
        }

        std::cout << "\n=== C++ FFI Example completed successfully ===\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    // Cleanup
    system("rm -rf ./cpp_example_data");

    return 0;
}