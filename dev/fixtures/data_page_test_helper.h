#pragma once

#include <memory>
#include <vector>
#include <string>
#include "../../data_page.h"
#include "../../data_page_builder.h"
#include "../../kv_options.h"
#include "../../comparator.h"

namespace eloqstore {
namespace test {

// Helper class to provide test-friendly interface to DataPage
class DataPageTestHelper {
public:
    // Create a DataPage from key-value pairs
    static std::unique_ptr<DataPage> BuildDataPage(
        const std::vector<std::pair<std::string, std::string>>& kvs,
        const KvOptions* options) {

        DataPageBuilder builder(options);

        for (const auto& [key, value] : kvs) {
            bool added = builder.Add(key, value, false, 0, 0);
            if (!added) {
                break; // Page full
            }
        }

        auto page_data = builder.Finish();

        // Create a DataPage with proper page ID
        auto page = std::make_unique<DataPage>(1);  // Use page ID 1 for tests

        // Allocate page memory
        Page page_mem(true);  // Allocate page
        std::memcpy(page_mem.Ptr(), page_data.data(), page_data.size());
        page->SetPage(std::move(page_mem));

        return page;
    }

    // Helper to count entries in a page using iterator
    static uint32_t NumEntries(const DataPage* page, const KvOptions* options) {
        DataPageIter iter(page, options);
        uint32_t count = 0;

        while (iter.HasNext()) {
            iter.Next();
            count++;
        }

        return count;
    }

    // Helper to check if page is empty
    static bool Empty(const DataPage* page) {
        return page->IsEmpty();
    }

    // Helper to get a value by key
    static bool Get(const DataPage* page, const KvOptions* options,
                     const std::string& key, std::string* value) {
        DataPageIter iter(page, options);

        if (iter.Seek(key)) {
            if (iter.Key() == key) {
                *value = std::string(iter.Value());
                return true;
            }
        }

        return false;
    }

    // Simple DataPage iterator wrapper for tests
    class Iterator {
    public:
        Iterator(const DataPage* page, const KvOptions* options)
            : iter_(page, options) {
            if (iter_.HasNext()) {
                iter_.Next();
            }
        }

        bool Valid() const {
            return valid_;
        }

        std::string_view key() const {
            return iter_.Key();
        }

        std::string_view value() const {
            return iter_.Value();
        }

        void Next() {
            if (iter_.HasNext()) {
                iter_.Next();
            } else {
                valid_ = false;
            }
        }

        void Seek(const std::string& target) {
            valid_ = iter_.Seek(target);
        }

        void SeekToFirst() {
            iter_.Reset();
            if (iter_.HasNext()) {
                iter_.Next();
                valid_ = true;
            } else {
                valid_ = false;
            }
        }

    private:
        DataPageIter iter_;
        bool valid_ = true;
    };
};

// Helper class for DataPageBuilder testing
class DataPageBuilderTestHelper {
public:
    static DataPageBuilder CreateBuilder(const KvOptions* options) {
        return DataPageBuilder(options);
    }

    static DataPageBuilder CreateBuilder(uint32_t page_size, uint32_t restart_interval) {
        KvOptions options;
        options.data_page_size = page_size;
        options.data_page_restart_interval = restart_interval;
        return DataPageBuilder(&options);
    }
};

} // namespace test
} // namespace eloqstore