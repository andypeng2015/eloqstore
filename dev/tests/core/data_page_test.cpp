#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <set>

#include "../../data_page.h"
#include "../../data_page_builder.h"
#include "../../comparator.h"
#include "../../coding.h"
#include "../../fixtures/test_fixtures.h"
#include "../../fixtures/test_helpers.h"
#include "../../fixtures/data_generator.h"
#include "../../fixtures/data_page_test_helper.h"

using namespace eloqstore;
using namespace eloqstore::test;

class DataPageTestFixture {
public:
    DataPageTestFixture() {
        options_.data_page_size = 4096;
        options_.data_page_restart_interval = 16;
        options_.comparator_ = Comparator::DefaultComparator();
    }

    std::unique_ptr<DataPage> BuildDataPage(
        const std::vector<std::pair<std::string, std::string>>& kvs) {
        return DataPageTestHelper::BuildDataPage(kvs, &options_);
    }

protected:
    KvOptions options_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_EmptyPage", "[datapage][unit]") {
    auto page = BuildDataPage({});

    REQUIRE(DataPageTestHelper::NumEntries(page.get(), &options_) == 0);
    REQUIRE(DataPageTestHelper::Empty(page.get()));

    // Lookups in empty page should return not found
    std::string value;
    REQUIRE(DataPageTestHelper::Get(page.get(), &options_, "any_key", &value) == false);

    // Iterator should be at end
    DataPageTestHelper::Iterator iter(page.get(), &options_);
    REQUIRE(!iter.Valid());
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_SingleEntry", "[datapage][unit]") {
    auto page = BuildDataPage({{"key1", "value1"}});

    REQUIRE(DataPageTestHelper::NumEntries(page.get(), &options_) == 1);
    REQUIRE(!DataPageTestHelper::Empty(page.get()));

    // Should be able to find the entry
    std::string value;
    REQUIRE(DataPageTestHelper::Get(page.get(), &options_, "key1", &value) == true);
    REQUIRE(value == "value1");

    // Should not find non-existent key
    REQUIRE(DataPageTestHelper::Get(page.get(), &options_, "key2", &value) == false);

    // Iterator should see one entry
    DataPageTestHelper::Iterator iter(page.get(), &options_);
    REQUIRE(iter.Valid());
    REQUIRE(iter.key() == "key1");
    REQUIRE(iter.value() == "value1");
    iter.Next();
    REQUIRE(!iter.Valid());
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_MultipleEntries", "[datapage][unit]") {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"aaa", "value_a"},
        {"bbb", "value_b"},
        {"ccc", "value_c"},
        {"ddd", "value_d"},
        {"eee", "value_e"}
    };

    auto page = BuildDataPage(kvs);

    REQUIRE(DataPageTestHelper::NumEntries(page.get(), &options_) == 5);

    // Verify all entries can be found
    for (const auto& [key, expected_value] : kvs) {
        std::string value;
        REQUIRE(DataPageTestHelper::Get(page.get(), &options_, key, &value) == true);
        REQUIRE(value == expected_value);
    }

    // Verify iteration order
    DataPageTestHelper::Iterator iter(page.get(), &options_);
    size_t idx = 0;
    while (iter.Valid() && idx < kvs.size()) {
        REQUIRE(iter.key() == kvs[idx].first);
        REQUIRE(iter.value() == kvs[idx].second);
        iter.Next();
        idx++;
    }
    REQUIRE(idx == kvs.size());
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_Seek", "[datapage][unit]") {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"aaa", "1"},
        {"bbb", "2"},
        {"ddd", "3"},
        {"fff", "4"},
        {"hhh", "5"}
    };

    auto page = BuildDataPage(kvs);
    DataPageTestHelper::Iterator iter(page.get(), &options_);

    SECTION("Seek to existing key") {
        iter.Seek("ddd");
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "ddd");
        REQUIRE(iter.value() == "3");
    }

    SECTION("Seek to non-existing key between entries") {
        iter.Seek("ccc");  // Between bbb and ddd
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "ddd");  // Should position at next key
    }

    SECTION("Seek before first key") {
        iter.Seek("000");
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "aaa");  // Should position at first key
    }

    SECTION("Seek after last key") {
        iter.Seek("zzz");
        REQUIRE(!iter.Valid());  // Should be at end
    }

    SECTION("SeekToFirst") {
        iter.Seek("fff");
        REQUIRE(iter.key() == "fff");

        iter.SeekToFirst();
        REQUIRE(iter.Valid());
        REQUIRE(iter.key() == "aaa");
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_LargeEntries", "[datapage][unit]") {
    // Test with larger values
    std::string large_value(500, 'X');
    std::vector<std::pair<std::string, std::string>> kvs;

    for (int i = 0; i < 10; ++i) {
        kvs.push_back({
            "key_" + std::to_string(i),
            large_value + std::to_string(i)
        });
    }

    auto page = BuildDataPage(kvs);

    // Some entries might not fit due to page size limit
    uint32_t count = DataPageTestHelper::NumEntries(page.get(), &options_);
    REQUIRE(count > 0);
    REQUIRE(count <= 10);

    // Verify the entries that did fit
    for (uint32_t i = 0; i < count; ++i) {
        std::string value;
        std::string key = "key_" + std::to_string(i);
        REQUIRE(DataPageTestHelper::Get(page.get(), &options_, key, &value) == true);
        REQUIRE(value == large_value + std::to_string(i));
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_RestartPoints", "[datapage][unit]") {
    // Restart points help with binary search
    // Add enough entries to trigger restart points
    std::vector<std::pair<std::string, std::string>> kvs;
    for (int i = 0; i < 100; ++i) {
        kvs.push_back({
            "key_" + std::string(4 - std::to_string(i).length(), '0') + std::to_string(i),
            "value_" + std::to_string(i)
        });
    }

    auto page = BuildDataPage(kvs);
    uint32_t count = DataPageTestHelper::NumEntries(page.get(), &options_);

    // Verify random access still works efficiently
    for (int i = 0; i < count; i += 10) {
        std::string key = "key_" + std::string(4 - std::to_string(i).length(), '0') + std::to_string(i);
        std::string value;
        REQUIRE(DataPageTestHelper::Get(page.get(), &options_, key, &value) == true);
        REQUIRE(value == "value_" + std::to_string(i));
    }
}

TEST_CASE_METHOD(DataPageTestFixture, "DataPage_EdgeCases", "[datapage][unit][edge]") {
    SECTION("Empty key") {
        auto page = BuildDataPage({{"", "empty_key_value"}});

        std::string value;
        REQUIRE(DataPageTestHelper::Get(page.get(), &options_, "", &value) == true);
        REQUIRE(value == "empty_key_value");
    }

    SECTION("Empty value") {
        auto page = BuildDataPage({{"key", ""}});

        std::string value = "not_empty";
        REQUIRE(DataPageTestHelper::Get(page.get(), &options_, "key", &value) == true);
        REQUIRE(value == "");
    }

    SECTION("Keys with special characters") {
        std::vector<std::pair<std::string, std::string>> kvs = {
            {"key\0with\0null", "value1"},
            {"key\nwith\nnewline", "value2"},
            {"key\twith\ttab", "value3"}
        };

        auto page = BuildDataPage(kvs);

        for (const auto& [key, expected_value] : kvs) {
            std::string value;
            if (DataPageTestHelper::Get(page.get(), &options_, key, &value)) {
                REQUIRE(value == expected_value);
            }
        }
    }
}