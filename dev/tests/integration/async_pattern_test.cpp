#include <catch2/catch_test_macros.hpp>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/test_helpers.h"
#include "../../eloq_store.h"
#include "../../types.h"

using namespace eloqstore;
using namespace eloqstore::test;
using namespace std::chrono_literals;

class AsyncPatternTestFixture : public TestFixture {
public:
    AsyncPatternTestFixture() {
        InitStoreWithDefaults();
        table_ = CreateTestTable("async_test");
    }
protected:
    TableIdent table_;
};

TEST_CASE_METHOD(AsyncPatternTestFixture, "AsyncPattern_IssueAndWait", "[integration][async]") {
    SECTION("Issue multiple async operations then wait") {
        const int num_writes = 50;
        std::vector<std::unique_ptr<BatchWriteRequest>> write_reqs;

        // Issue all async writes first
        for (int i = 0; i < num_writes; ++i) {
            auto req = std::make_unique<BatchWriteRequest>();
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            std::string value = "value_" + std::to_string(i);

            req->AddWrite(key, value, 0, WriteOp::Upsert);
            req->SetTableId(table_);

            // Execute async
            store_->ExecAsyn(req.get());
            write_reqs.push_back(std::move(req));
        }

        // Now wait for all to complete
        for (auto& req : write_reqs) {
            while (!req->IsDone()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            REQUIRE(req->Error() == KvError::NoError);
        }

        // Verify all data with sync reads
        for (int i = 0; i < num_writes; ++i) {
            auto read_req = std::make_unique<ReadRequest>();
            std::string key = std::string(10 - std::to_string(i).length(), '0') + std::to_string(i);
            read_req->SetArgs(table_, key);

            store_->ExecSync(read_req.get());
            REQUIRE(read_req->Error() == KvError::NoError);

            std::string expected_value = "value_" + std::to_string(i);
            REQUIRE(read_req->value_ == expected_value);
        }
    }

    SECTION("Mixed async reads and writes") {
        // Write initial data synchronously
        for (int i = 0; i < 20; ++i) {
            auto write_req = std::make_unique<BatchWriteRequest>();
            std::string key = "key_" + std::to_string(i);
            std::string value = "initial_" + std::to_string(i);

            write_req->AddWrite(key, value, 0, WriteOp::Upsert);
            write_req->SetTableId(table_);

            store_->ExecSync(write_req.get());
            REQUIRE(write_req->Error() == KvError::NoError);
        }

        // Now issue mixed async reads and writes
        std::vector<std::unique_ptr<KvRequest>> all_reqs;

        for (int i = 0; i < 40; ++i) {
            if (i % 2 == 0) {
                // Async read
                auto req = std::make_unique<ReadRequest>();
                req->SetArgs(table_, "key_" + std::to_string(i / 2));
                store_->ExecAsyn(req.get());
                all_reqs.push_back(std::move(req));
            } else {
                // Async update
                auto req = std::make_unique<BatchWriteRequest>();
                std::string key = "key_" + std::to_string(i / 2);
                std::string value = "updated_" + std::to_string(i);

                req->AddWrite(key, value, 0, WriteOp::Upsert);
                req->SetTableId(table_);

                store_->ExecAsyn(req.get());
                all_reqs.push_back(std::move(req));
            }
        }

        // Wait for all to complete
        for (auto& req : all_reqs) {
            while (!req->IsDone()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            // Note: Some reads might return old values depending on timing
            // This is expected behavior in async scenarios
        }
    }
}

TEST_CASE_METHOD(AsyncPatternTestFixture, "AsyncPattern_Pipelining", "[integration][async]") {
    SECTION("Pipeline writes with dependencies") {
        const int pipeline_depth = 10;
        const int operations_per_stage = 5;

        for (int stage = 0; stage < pipeline_depth; ++stage) {
            std::vector<std::unique_ptr<BatchWriteRequest>> stage_reqs;

            // Issue operations for this stage
            for (int op = 0; op < operations_per_stage; ++op) {
                auto req = std::make_unique<BatchWriteRequest>();
                std::string key = "stage_" + std::to_string(stage) + "_op_" + std::to_string(op);
                std::string value = "value_s" + std::to_string(stage) + "_o" + std::to_string(op);

                req->AddWrite(key, value, 0, WriteOp::Upsert);
                req->SetTableId(table_);

                store_->ExecAsyn(req.get());
                stage_reqs.push_back(std::move(req));
            }

            // Wait for this stage to complete before next
            for (auto& req : stage_reqs) {
                while (!req->IsDone()) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
                REQUIRE(req->Error() == KvError::NoError);
            }
        }

        // Verify all data
        for (int stage = 0; stage < pipeline_depth; ++stage) {
            for (int op = 0; op < operations_per_stage; ++op) {
                auto read_req = std::make_unique<ReadRequest>();
                std::string key = "stage_" + std::to_string(stage) + "_op_" + std::to_string(op);
                read_req->SetArgs(table_, key);

                store_->ExecSync(read_req.get());
                REQUIRE(read_req->Error() == KvError::NoError);
            }
        }
    }
}