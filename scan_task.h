#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "data_page.h"
#include "error.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

using KvEntry = std::tuple<std::string, std::string, uint64_t>;

class ScanTask : public KvTask
{
public:
    ScanTask();

    KvError Scan(const TableIdent &tbl_id,
                 std::string_view begin_key,
                 std::string_view end_key,
                 std::vector<KvEntry> &entries);

    // TODO(zhanghao): remove this unused API ?
    KvError Iterate(const TableIdent &tbl_id,
                    std::string_view begin_key,
                    std::string_view end_key);
    bool Valid() const;
    std::string_view Key() const;
    std::string_view Value() const;
    uint64_t Timestamp() const;

    KvError Next(MappingSnapshot *m);
    TaskType Type() const override
    {
        return TaskType::Scan;
    }

private:
    KvError NextPage(MappingSnapshot *m);
    DataPage data_page_;

    std::shared_ptr<MappingSnapshot> mapping_;
    DataPageIter iter_;
    std::string end_key_;
};
}  // namespace kvstore