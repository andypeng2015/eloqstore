#pragma once

#include <vector>

#include "kv_options.h"
#include "mem_index_page.h"

namespace eloqstore
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

    MemIndexPage *idx_page_{nullptr};  // 索引页面指针
    IndexPageIter idx_page_iter_;      // 页面迭代器
    std::vector<IndexOp> changes_{};   // 该层级的变更操作
    bool is_leaf_index_{false};        // 是否为叶子索引层
};
}  // namespace eloqstore