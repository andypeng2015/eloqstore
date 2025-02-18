#pragma once

#include <string_view>
#include <vector>

#include "kv_options.h"
#include "mem_index_page.h"
#include "write_op.h"

namespace kvstore
{
struct IndexOp
{
    std::string key_;
    uint32_t page_id_;
    WriteOp op_;
};

class IndexStackEntry
{
public:
    IndexStackEntry(MemIndexPage *page, const KvOptions *opts)
        : idx_page_(page), idx_page_iter_(page, opts)
    {
    }

    IndexStackEntry(const IndexStackEntry &) = delete;
    IndexStackEntry(IndexStackEntry &&rhs) = delete;

    MemIndexPage *idx_page_{nullptr};
    IndexPageIter idx_page_iter_;
    std::vector<IndexOp> changes_{};
    bool is_leaf_index_{false};
};

class WriteIndexStack
{
public:
    WriteIndexStack(IndexPageManager *idx_page_manager);

    void Seek(MemIndexPage *root, std::string_view key);

    void Pop();

private:
    std::vector<IndexStackEntry> stack_;
    IndexPageManager *idx_page_manager_{nullptr};
};
}  // namespace kvstore