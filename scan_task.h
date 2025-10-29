#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "data_page.h"
#include "error.h"
#include "task.h"
#include "types.h"

namespace eloqstore
{
class ScanIterator
{
public:
    ScanIterator(const TableIdent &tbl_id);
    KvError Seek(std::string_view key, bool ttl = false);
    KvError Next();

    std::string_view Key() const;
    std::pair<std::string_view, KvError> ResolveValue(std::string &storage);
    uint64_t ExpireTs() const;
    uint64_t Timestamp() const;

    MappingSnapshot *Mapping() const;

private:
    static constexpr size_t kPrefetchPageCount = 5;

    const TableIdent tbl_id_;
    std::shared_ptr<MappingSnapshot> mapping_;
    std::vector<DataPage> prefetched_pages_;
    size_t prefetched_offset_{0};
    DataPage data_page_;
    DataPageIter iter_;
    const compression::DictCompression *compression_{nullptr};

    KvError PrefetchPages(PageId root_id, std::string_view key);
};

class ScanRequest;
class ScanTask : public KvTask
{
public:
    KvError Scan();
    TaskType Type() const override
    {
        return TaskType::Scan;
    }
};
}  // namespace eloqstore
