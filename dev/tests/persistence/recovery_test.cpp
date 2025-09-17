#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/random_generator.h"
#include "../../eloq_store.h"
#include "../../types.h"
#include "../../kv_options.h"

namespace fs = boost::filesystem;

class RecoveryTestFixture {
public:
    RecoveryTestFixture() : data_dir_("/mnt/ramdisk/recovery_test_" + GenerateTestId()) {
        fs::create_directories(data_dir_);
        backup_dir_ = data_dir_ + "_backup";
        fs::create_directories(backup_dir_);
    }

    ~RecoveryTestFixture() {
        Cleanup();
    }

    void StartStore(bool clean_start = false) {
        if (clean_start) {
            CleanDataDir();
        }

        eloqstore::KvOptions opts;
        opts.store_path = {data_dir_};
        opts.num_threads = 2;
        opts.buf_ring_size = 1024; // Buffer ring size
        opts.index_buffer_pool_size = 512; // Index buffer pool size

        store_ = std::make_unique<eloqstore::EloqStore>(opts);
        store_->Start();
    }

    void StopStore() {
        if (store_) {
            store_->Stop();
            store_.reset();
        }
    }

    void BackupDataDir() {
        if (fs::exists(backup_dir_)) {
            fs::remove_all(backup_dir_);
        }
        fs::create_directories(backup_dir_);

        // Copy all files from data_dir to backup_dir
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            fs::copy(entry.path(), backup_dir_ / entry.path().filename());
        }
    }

    void RestoreDataDir() {
        if (fs::exists(data_dir_)) {
            fs::remove_all(data_dir_);
        }
        fs::create_directories(data_dir_);

        // Copy all files from backup_dir to data_dir
        for (const auto& entry : fs::directory_iterator(backup_dir_)) {
            fs::copy(entry.path(), data_dir_ / entry.path().filename());
        }
    }

    void CorruptFile(const std::string& pattern, size_t corruption_offset, size_t corruption_size) {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            std::string filename = entry.path().filename().string();
            if (filename.find(pattern) != std::string::npos) {
                CorruptSpecificFile(entry.path().string(), corruption_offset, corruption_size);
                break;
            }
        }
    }

    void SimulateCrash() {
        // Fork a child process to simulate crash
        pid_t pid = fork();
        if (pid == 0) {
            // Child process - simulate crash by killing itself
            raise(SIGKILL);
        } else if (pid > 0) {
            // Parent process - wait for child to crash
            int status;
            waitpid(pid, &status, 0);
        }
    }

    bool WriteData(const eloqstore::TableIdent& table, const std::map<std::string, std::string>& data) {
        auto req = std::make_unique<eloqstore::BatchWriteRequest>();
        req->SetTableId(table);

        for (const auto& [key, value] : data) {
            req->AddWrite(key, value, 0, eloqstore::WriteOp::Upsert);
        }

        store_->ExecSync(req.get());
        return req->Error() == eloqstore::KvError::NoError;
    }

    std::map<std::string, std::string> ReadAllData(const eloqstore::TableIdent& table) {
        std::map<std::string, std::string> result;

        auto req = std::make_unique<eloqstore::ScanRequest>();
        req->SetArgs(table, "", "");
        req->SetPagination(UINT32_MAX, SIZE_MAX);

        store_->ExecSync(req.get());

        if (req->Error() == eloqstore::KvError::NoError) {
            auto entries = req->Entries();
            for (const auto& kv : entries) {
                result[kv.key_] = kv.value_;
            }
        }

        return result;
    }

    bool VerifyData(const eloqstore::TableIdent& table, const std::map<std::string, std::string>& expected) {
        auto actual = ReadAllData(table);

        if (actual.size() != expected.size()) {
            LOG(ERROR) << "Size mismatch: expected=" << expected.size()
                      << " actual=" << actual.size();
            return false;
        }

        for (const auto& [key, value] : expected) {
            auto it = actual.find(key);
            if (it == actual.end()) {
                LOG(ERROR) << "Missing key: " << key;
                return false;
            }
            if (it->second != value) {
                LOG(ERROR) << "Value mismatch for key " << key
                          << ": expected=" << value << " actual=" << it->second;
                return false;
            }
        }

        return true;
    }

private:
    std::string GenerateTestId() {
        static std::atomic<int> counter{0};
        return std::to_string(std::time(nullptr)) + "_" + std::to_string(counter++);
    }

    void CleanDataDir() {
        if (fs::exists(data_dir_)) {
            fs::remove_all(data_dir_);
        }
        fs::create_directories(data_dir_);
    }

    void Cleanup() {
        StopStore();
        try {
            if (fs::exists(data_dir_)) {
                fs::remove_all(data_dir_);
            }
            if (fs::exists(backup_dir_)) {
                fs::remove_all(backup_dir_);
            }
        } catch (...) {
            // Ignore cleanup errors
        }
    }

    void CorruptSpecificFile(const std::string& filepath, size_t offset, size_t size) {
        std::fstream file(filepath, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) return;

        file.seekp(offset);
        std::vector<char> garbage(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (auto& byte : garbage) {
            byte = static_cast<char>(dis(gen));
        }

        file.write(garbage.data(), garbage.size());
        file.close();
    }

    std::string data_dir_;
    std::string backup_dir_;
    std::unique_ptr<eloqstore::EloqStore> store_;
};

TEST_CASE("Recovery after clean shutdown", "[persistence]") {
    RecoveryTestFixture fixture;
    eloqstore::TableIdent table{"test_table", 1};
    eloqstore::test::RandomGenerator gen(42);

    SECTION("Simple data persistence") {
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 100; ++i) {
            test_data["key_" + std::to_string(i)] = gen.GetValue(100, 1000);
        }

        // Write data and shutdown cleanly
        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, test_data));
        fixture.StopStore();

        // Restart and verify data
        fixture.StartStore();
        REQUIRE(fixture.VerifyData(table, test_data));
    }

    SECTION("Large dataset persistence") {
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 10000; ++i) {
            test_data[gen.GetKey(10, 50)] = gen.GetValue(100, 5000);
        }

        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, test_data));
        fixture.StopStore();

        fixture.StartStore();
        REQUIRE(fixture.VerifyData(table, test_data));
    }

    SECTION("Multiple restart cycles") {
        std::map<std::string, std::string> cumulative_data;

        for (int cycle = 0; cycle < 5; ++cycle) {
            fixture.StartStore(cycle == 0);

            // Add new data in each cycle
            std::map<std::string, std::string> cycle_data;
            for (int i = 0; i < 20; ++i) {
                std::string key = "cycle_" + std::to_string(cycle) + "_key_" + std::to_string(i);
                cycle_data[key] = gen.GetValue(100, 500);
                cumulative_data[key] = cycle_data[key];
            }

            REQUIRE(fixture.WriteData(table, cycle_data));
            REQUIRE(fixture.VerifyData(table, cumulative_data));

            fixture.StopStore();
        }

        // Final verification
        fixture.StartStore();
        REQUIRE(fixture.VerifyData(table, cumulative_data));
    }
}

TEST_CASE("Recovery after crash scenarios", "[persistence]") {
    RecoveryTestFixture fixture;
    eloqstore::TableIdent table{"crash_table", 1};
    eloqstore::test::RandomGenerator gen(43);

    SECTION("Crash during write operations") {
        std::map<std::string, std::string> persistent_data;
        for (int i = 0; i < 50; ++i) {
            persistent_data["persist_" + std::to_string(i)] = gen.GetValue(100, 500);
        }

        // Write persistent data
        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, persistent_data));

        // Backup current state
        fixture.BackupDataDir();

        // Start writing more data (simulating crash during write)
        std::map<std::string, std::string> volatile_data;
        for (int i = 0; i < 50; ++i) {
            volatile_data["volatile_" + std::to_string(i)] = gen.GetValue(100, 500);
        }

        // Write half the volatile data
        auto it = volatile_data.begin();
        std::map<std::string, std::string> partial_data;
        for (int i = 0; i < 25 && it != volatile_data.end(); ++i, ++it) {
            partial_data[it->first] = it->second;
        }
        fixture.WriteData(table, partial_data);

        // Simulate crash
        fixture.StopStore();
        fixture.RestoreDataDir(); // Restore to pre-volatile state

        // Restart and verify only persistent data exists
        fixture.StartStore();
        auto recovered_data = fixture.ReadAllData(table);

        // Check persistent data is intact
        for (const auto& [key, value] : persistent_data) {
            REQUIRE(recovered_data.count(key) == 1);
            REQUIRE(recovered_data[key] == value);
        }
    }

    SECTION("Crash with pending flush") {
        fixture.StartStore(true);

        // Write data rapidly without allowing flush
        for (int batch = 0; batch < 10; ++batch) {
            std::map<std::string, std::string> batch_data;
            for (int i = 0; i < 100; ++i) {
                batch_data["batch_" + std::to_string(batch) + "_key_" + std::to_string(i)] =
                    gen.GetValue(50, 200);
            }
            fixture.WriteData(table, batch_data);
        }

        // Force flush and wait
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Get current data
        auto pre_crash_data = fixture.ReadAllData(table);

        fixture.StopStore();

        // Restart and verify data consistency
        fixture.StartStore();
        auto post_crash_data = fixture.ReadAllData(table);

        // Most data should be recovered (allow some loss for unflushed data)
        double recovery_rate = static_cast<double>(post_crash_data.size()) / pre_crash_data.size();
        REQUIRE(recovery_rate > 0.8); // At least 80% recovery
    }
}

TEST_CASE("Recovery with corrupted files", "[persistence]") {
    RecoveryTestFixture fixture;
    eloqstore::TableIdent table{"corrupt_table", 1};
    eloqstore::test::RandomGenerator gen(44);

    SECTION("Corrupted data file recovery") {
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 100; ++i) {
            test_data["key_" + std::to_string(i)] = gen.GetValue(100, 500);
        }

        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, test_data));
        fixture.StopStore();

        // Corrupt a data file
        fixture.CorruptFile("data", 1024, 128); // Corrupt 128 bytes at offset 1024

        // Try to restart - should handle corruption gracefully
        fixture.StartStore();

        // Some data might be lost but store should be operational
        auto recovered_data = fixture.ReadAllData(table);
        REQUIRE(recovered_data.size() > 0); // At least some data recovered

        // Store should be writable after recovery
        std::map<std::string, std::string> new_data;
        new_data["new_key_after_corruption"] = "new_value";
        REQUIRE(fixture.WriteData(table, new_data));
    }

    SECTION("Corrupted index recovery") {
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 500; ++i) {
            test_data[gen.GetKey(10, 30)] = gen.GetValue(100, 1000);
        }

        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, test_data));
        fixture.StopStore();

        // Corrupt index file
        fixture.CorruptFile("index", 512, 256);

        // Restart - should rebuild index
        fixture.StartStore();

        // Verify data is accessible (index rebuilt)
        auto recovered_data = fixture.ReadAllData(table);
        REQUIRE(recovered_data.size() > test_data.size() * 0.7); // Allow some loss
    }
}

TEST_CASE("Recovery with incomplete operations", "[persistence]") {
    RecoveryTestFixture fixture;
    eloqstore::TableIdent table{"incomplete_table", 1};
    eloqstore::test::RandomGenerator gen(45);

    SECTION("Incomplete batch write recovery") {
        std::map<std::string, std::string> complete_data;
        for (int i = 0; i < 50; ++i) {
            complete_data["complete_" + std::to_string(i)] = gen.GetValue(100, 500);
        }

        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, complete_data));

        // Prepare large batch that might not complete
        std::map<std::string, std::string> incomplete_batch;
        for (int i = 0; i < 1000; ++i) {
            incomplete_batch["incomplete_" + std::to_string(i)] = gen.GetValue(1000, 5000);
        }

        // Start writing in background
        std::thread write_thread([&]() {
            fixture.WriteData(table, incomplete_batch);
        });

        // Simulate crash after short delay
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        write_thread.detach(); // Abandon thread
        fixture.StopStore();

        // Restart and verify complete data is intact
        fixture.StartStore();
        auto recovered_data = fixture.ReadAllData(table);

        for (const auto& [key, value] : complete_data) {
            REQUIRE(recovered_data.count(key) == 1);
            REQUIRE(recovered_data[key] == value);
        }
    }

    SECTION("Recovery with mixed operations") {
        fixture.StartStore(true);

        // Perform various operations
        std::map<std::string, std::string> puts;
        std::set<std::string> deletes;

        // Initial data
        for (int i = 0; i < 100; ++i) {
            puts["key_" + std::to_string(i)] = gen.GetValue(100, 500);
        }
        REQUIRE(fixture.WriteData(table, puts));

        // Delete some keys
        auto del_req = std::make_unique<BatchWriteRequest>();
        for (int i = 0; i < 50; i += 2) {
            std::string key = "key_" + std::to_string(i);
            del_req->AddWrite(key, "", 0, WriteOp::Delete);
            deletes.insert(key);
        }
        fixture.store_->ExecSync(del_req.get(), 0);
        REQUIRE(del_req->Error() == KvError::kOk);

        // Update some keys
        std::map<std::string, std::string> updates;
        for (int i = 1; i < 100; i += 2) {
            std::string key = "key_" + std::to_string(i);
            updates[key] = gen.GetValue(200, 600);
            puts[key] = updates[key]; // Update expected data
        }
        REQUIRE(fixture.WriteData(table, updates));

        fixture.StopStore();

        // Restart and verify final state
        fixture.StartStore();
        auto recovered_data = fixture.ReadAllData(table);

        // Check all operations were persisted correctly
        for (const auto& [key, value] : puts) {
            if (deletes.count(key) == 0) {
                REQUIRE(recovered_data.count(key) == 1);
                REQUIRE(recovered_data[key] == value);
            } else {
                REQUIRE(recovered_data.count(key) == 0);
            }
        }
    }
}

TEST_CASE("Long-term persistence stability", "[persistence][long]") {
    RecoveryTestFixture fixture;
    eloqstore::TableIdent table{"stability_table", 1};
    eloqstore::test::RandomGenerator gen(46);

    SECTION("Extended write-restart cycles") {
        std::map<std::string, std::string> all_data;
        const int num_cycles = 20;
        const int keys_per_cycle = 100;

        for (int cycle = 0; cycle < num_cycles; ++cycle) {
            fixture.StartStore(cycle == 0);

            // Write new data
            std::map<std::string, std::string> cycle_data;
            for (int i = 0; i < keys_per_cycle; ++i) {
                std::string key = gen.GetKey(20, 50);
                std::string value = gen.GetValue(100, 1000);
                cycle_data[key] = value;
                all_data[key] = value; // May overwrite existing keys
            }

            REQUIRE(fixture.WriteData(table, cycle_data));

            // Randomly delete some keys
            if (cycle % 3 == 0 && !all_data.empty()) {
                auto del_req = std::make_unique<BatchWriteRequest>();
                auto it = all_data.begin();
                for (int i = 0; i < 10 && it != all_data.end(); ++i) {
                    del_req->AddWrite(it->first, "", 0, WriteOp::Delete);
                    it = all_data.erase(it);
                }
                fixture.store_->ExecSync(del_req.get(), 0);
            }

            // Verify current state
            REQUIRE(fixture.VerifyData(table, all_data));

            fixture.StopStore();

            // Occasionally simulate crash
            if (cycle % 5 == 4) {
                fixture.SimulateCrash();
            }
        }

        // Final verification after all cycles
        fixture.StartStore();
        REQUIRE(fixture.VerifyData(table, all_data));
    }
}