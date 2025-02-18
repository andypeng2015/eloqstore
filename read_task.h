#pragma once

#include <string_view>
#include <unordered_set>

#include "data_page.h"
#include "error.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

class ReadTask : public KvTask
{
public:
    void Reset(IndexPageManager *idx_page_manager);

    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string_view &value,
                 uint64_t &timestamp);

    TaskType Type() const override
    {
        return TaskType::Read;
    }

private:
    DataPage data_page_;

public:
    /**
     * @brief A collection of pages the task has requested to read. A task
     * usually issues one read request and waits for it (BlockedForOne). When a
     * value spans multiple pages, the task may issue a batch of requests and
     * wait for all of them to finish (BlockedForAll).
     *
     */
    std::unordered_set<uint32_t> read_fps_;
};
}  // namespace kvstore