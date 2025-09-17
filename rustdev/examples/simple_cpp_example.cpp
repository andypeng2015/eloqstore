/**
 * Simple C++ FFI Example for EloqStore
 *
 * This example demonstrates basic usage of EloqStore from C++ through the FFI interface.
 * It shows only write and read operations which are currently exported.
 */

#include <iostream>
#include <cstring>
#include <chrono>
#include <memory>
#include <cassert>

// Include the FFI header
extern "C" {
#include "../include/eloqstore.h"
}

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
    std::cout << "=== EloqStore Simple C++ FFI Example ===\n\n";

    // 1. Initialize store
    std::cout << "1. Initializing EloqStore with data directory: ./cpp_example_data\n";
    EloqStoreHandle* store = eloqstore_new("./cpp_example_data");

    if (!store) {
        std::cerr << "   Failed to create EloqStore\n";
        return 1;
    }
    std::cout << "   Store created successfully!\n\n";

    // 2. Start the store
    std::cout << "2. Starting the store...\n";
    if (eloqstore_start(store) != 0) {
        std::cerr << "   Failed to start store\n";
        eloqstore_free(store);
        return 1;
    }
    std::cout << "   Store started successfully!\n\n";

    const char* table = "users";

    // 3. Write some data
    std::cout << "3. Writing key-value pairs:\n";
    {
        // Write user:1001
        const char* key1 = "user:1001";
        const char* value1 = R"({"name":"Alice","age":30})";

        Timer timer;
        int result = eloqstore_write(
            store,
            table,
            (const uint8_t*)key1, strlen(key1),
            (const uint8_t*)value1, strlen(value1)
        );

        if (result == 0) {
            std::cout << "   Written: " << key1 << " -> " << value1 << "\n";
            std::cout << "   Write latency: " << timer.elapsed_ms() << "ms\n";
        } else {
            std::cerr << "   Write failed for " << key1 << "\n";
        }

        // Write user:1002
        const char* key2 = "user:1002";
        const char* value2 = R"({"name":"Bob","age":25})";

        result = eloqstore_write(
            store,
            table,
            (const uint8_t*)key2, strlen(key2),
            (const uint8_t*)value2, strlen(value2)
        );

        if (result == 0) {
            std::cout << "   Written: " << key2 << " -> " << value2 << "\n";
        }
    }
    std::cout << "\n";

    // 4. Read the data back
    std::cout << "4. Reading keys back:\n";
    {
        // Read user:1001
        const char* key1 = "user:1001";
        uint8_t* value_out = nullptr;
        size_t value_len = 0;

        Timer timer;
        int result = eloqstore_read(
            store,
            table,
            (const uint8_t*)key1, strlen(key1),
            &value_out, &value_len
        );

        std::cout << "   Read latency: " << timer.elapsed_ms() << "ms\n";

        if (result == 0 && value_out != nullptr) {
            std::string value(reinterpret_cast<char*>(value_out), value_len);
            std::cout << "   Found " << key1 << " -> " << value << "\n";

            // Free the value
            eloqstore_free_value(value_out, value_len);
        } else if (result == 1) {
            std::cout << "   Key " << key1 << " not found\n";
        } else {
            std::cerr << "   Read failed for " << key1 << "\n";
        }

        // Read user:1002
        const char* key2 = "user:1002";
        value_out = nullptr;
        value_len = 0;

        result = eloqstore_read(
            store,
            table,
            (const uint8_t*)key2, strlen(key2),
            &value_out, &value_len
        );

        if (result == 0 && value_out != nullptr) {
            std::string value(reinterpret_cast<char*>(value_out), value_len);
            std::cout << "   Found " << key2 << " -> " << value << "\n";
            eloqstore_free_value(value_out, value_len);
        }

        // Try reading a non-existent key
        const char* key3 = "user:9999";
        value_out = nullptr;
        value_len = 0;

        result = eloqstore_read(
            store,
            table,
            (const uint8_t*)key3, strlen(key3),
            &value_out, &value_len
        );

        if (result == 1) {
            std::cout << "   Key " << key3 << " not found (as expected)\n";
        }
    }
    std::cout << "\n";

    // 5. Update a key
    std::cout << "5. Updating user:1001:\n";
    {
        const char* key = "user:1001";
        const char* new_value = R"({"name":"Alice Smith","age":31})";

        int result = eloqstore_write(
            store,
            table,
            (const uint8_t*)key, strlen(key),
            (const uint8_t*)new_value, strlen(new_value)
        );

        if (result == 0) {
            std::cout << "   Updated " << key << " with new data\n";
        }
    }
    std::cout << "\n";

    // 6. Performance test
    std::cout << "6. Performance test - 100 writes:\n";
    {
        Timer total_timer;
        int success = 0;

        for (int i = 0; i < 100; i++) {
            std::string key = "test:" + std::to_string(i);
            std::string value = "value" + std::to_string(i);

            int result = eloqstore_write(
                store,
                table,
                (const uint8_t*)key.c_str(), key.length(),
                (const uint8_t*)value.c_str(), value.length()
            );

            if (result == 0) {
                success++;
            }
        }

        double total_ms = total_timer.elapsed_ms();
        std::cout << "   Successful writes: " << success << "/100\n";
        std::cout << "   Total time: " << total_ms << "ms\n";
        std::cout << "   Average latency: " << total_ms / 100.0 << "ms\n";
        if (total_ms > 0) {
            std::cout << "   Throughput: " << 100.0 / (total_ms / 1000.0) << " ops/sec\n";
        }
    }
    std::cout << "\n";

    // 7. Shutdown
    std::cout << "7. Shutting down store...\n";
    if (eloqstore_stop(store) == 0) {
        std::cout << "   Store stopped successfully!\n";
    } else {
        std::cerr << "   Failed to stop store\n";
    }

    // 8. Free resources
    eloqstore_free(store);
    std::cout << "   Resources freed\n";

    std::cout << "\n=== C++ FFI Example completed successfully ===\n";

    // Cleanup
    system("rm -rf ./cpp_example_data");

    return 0;
}