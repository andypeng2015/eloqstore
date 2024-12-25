#include "write_task.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "data_page.h"
#include "error.h"
#include "global_variables.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "page_type.h"
#include "storage_manager.h"
#include "write_op.h"

namespace kvstore
{
WriteTask::WriteTask(std::string tbl_name,
                     uint32_t partition_id,
                     const KvOptions *opt)
    : tbl_ident_(tbl_name, partition_id),
      idx_page_builder_(opt),
      data_page_builder_(opt),
      write_reqs_(opt->task_write_buffer)
{
    for (auto &req : write_reqs_)
    {
        RecycleWriteReq(&req);
    }
}

void WriteTask::Yield()
{
}

void WriteTask::Resume()
{
}

void WriteTask::Rollback()
{
}

void WriteTask::Reset(IndexPageManager *idx_page_manager)
{
    data_.clear();
    stack_.clear();
    idx_page_manager_ = idx_page_manager;
}

void WriteTask::AddData(std::string key,
                        std::string val,
                        uint64_t ts,
                        WriteOp op)
{
    assert(data_.empty() || data_.back().key_ < key);
    data_.emplace_back(std::move(key), std::move(val), ts, op);
}

MemIndexPage *WriteTask::Pop()
{
    if (stack_.empty())
    {
        return nullptr;
    }

    IndexStackEntry *stack_entry = stack_.back().get();
    // There is no change at this level.
    if (stack_entry->changes_.empty())
    {
        MemIndexPage *page = stack_entry->idx_page_;
        if (page != nullptr)
        {
            page->Unpin();
        }
        stack_.pop_back();
        return page;
    }

    idx_page_builder_.Reset();

    const Comparator *cmp = idx_page_manager_->GetComparator();
    std::vector<IndexOp> &changes = stack_entry->changes_;
    MemIndexPage *stack_page = stack_entry->idx_page_;
    // If the change op contains no index page pointer, this is the lowest level
    // index page.
    bool is_leaf_index = stack_entry->is_leaf_index_;
    IndexPageIter base_page_iter{stack_page, cmp};
    bool is_base_iter_valid = false;
    AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);

    idx_page_builder_.Reset();

    // Merges index entries in the page with the change vector.
    auto cit = changes.begin();

    // We keep the previous built page in the pipeline before flushing it to
    // storage. This is to redistribute between last two pages in case the last
    // page is sparse.
    MemIndexPage *prev_page = nullptr;
    MemIndexPage *curr_page = nullptr;
    std::string prev_page_key{};
    std::string_view page_key =
        stack_.size() == 1 ? std::string_view{}
                           : stack_[stack_.size() - 2]->idx_page_iter_.Key();
    std::string curr_page_key{page_key};

    uint32_t page_id = UINT32_MAX;
    uint32_t file_page_id = UINT32_MAX;
    if (stack_page != nullptr)
    {
        page_id = stack_page->PageId();
        file_page_id = stack_page->FilePageId();
    }

    auto add_to_page = [&](std::string_view new_key, uint32_t new_page_id)
    {
        bool success =
            idx_page_builder_.Add(new_key, new_page_id, is_leaf_index);
        if (!success)
        {
            curr_page = idx_page_manager_->AllocIndexPage();
            if (curr_page == nullptr)
            {
                // Rollback the task.
                thd_task->Rollback();
                assert("A rollback task should not continue.");
            }
            // The page is full.
            std::string_view page_view = idx_page_builder_.Finish();
            memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());

            if (prev_page != nullptr)
            {
                // The update results in an index page split, because there is
                // at least one new index page to come. Flushes the previously
                // built index page and elevates it to the parent in the stack.
                FinishIndexPage(prev_page,
                                std::move(prev_page_key),
                                page_id,
                                file_page_id,
                                true);

                // The first split index page shares the same page Id with the
                // original one. The following index pages will have new page
                // Id's.
                page_id = UINT32_MAX;
                file_page_id = UINT32_MAX;
            }

            prev_page = curr_page;
            prev_page_key = std::move(curr_page_key);
            curr_page_key = new_key;
            idx_page_builder_.Reset();
            // The first index entry is the leftmost pointer w/o the key.
            idx_page_builder_.Add(
                std::string_view{}, new_page_id, is_leaf_index);
        }
    };

    while (is_base_iter_valid && cit != changes.end())
    {
        std::string_view base_key = base_page_iter.Key();
        uint32_t base_page_id = base_page_iter.PageId();
        std::string_view change_key{cit->key_.data(), cit->key_.size()};
        uint32_t change_page = cit->page_id_;
        int cmp_ret = cmp->Compare(base_key, change_key);

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        uint32_t new_page_id;
        AdvanceType adv_type;

        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_page_id = base_page_id;
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (cit->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
                new_page_id = UINT32_MAX;
            }
            else
            {
                new_key = change_key;
                new_page_id = change_page;
            }
        }
        else
        {
            // base_key > change_key
            assert(cit->op_ == WriteOp::Upsert);
            adv_type = AdvanceType::Changes;
            new_key = change_key;
            new_page_id = change_page;
        }

        // The first inserted entry is the leftmost pointer whose key is empty.
        if (!new_key.empty() || new_page_id != UINT32_MAX)
        {
            add_to_page(new_key, new_page_id);
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            cit++;
            break;
        default:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            cit++;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        std::string_view new_key = base_page_iter.Key();
        uint32_t new_page_id = base_page_iter.PageId();
        add_to_page(new_key, new_page_id);
        AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
    }

    while (cit != changes.end())
    {
        if (cit->op_ != WriteOp::Delete)
        {
            std::string_view new_key{cit->key_.data(), cit->key_.size()};
            uint32_t new_page = cit->page_id_;
            add_to_page(new_key, new_page);
        }
        ++cit;
    }

    bool elevate;
    if (prev_page != nullptr)
    {
        FinishIndexPage(
            prev_page, std::move(prev_page_key), page_id, file_page_id, true);
        page_id = UINT32_MAX;
        file_page_id = UINT32_MAX;
        // The index page is split into two or more pages, all of which are
        // elevated.
        elevate = true;
    }
    else
    {
        // The update does not yield a page split. The new page has the same
        // page Id as the original one. It is mapped to a new file page and is
        // not elevated to its parent page.
        elevate = false;
    }

    if (idx_page_builder_.IsEmpty())
    {
        FreePage(stack_.back()->idx_page_->PageId());
        if (stack_.size() > 1)
        {
            IndexStackEntry *parent = stack_[stack_.size() - 2].get();
            std::string_view page_key = parent->idx_page_iter_.Key();
            parent->changes_.emplace_back(
                std::string(page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        curr_page = idx_page_manager_->AllocIndexPage();
        if (curr_page == nullptr)
        {
            // Rollback the task.
            thd_task->Rollback();
            assert("A rollback task should not continue.");
        }
        std::string_view page_view = idx_page_builder_.Finish();
        memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());
        FinishIndexPage(curr_page,
                        std::move(curr_page_key),
                        page_id,
                        file_page_id,
                        elevate);
    }

    if (stack_page != nullptr)
    {
        stack_page->Unpin();
    }
    stack_.pop_back();
    return curr_page;
}

void WriteTask::FinishIndexPage(MemIndexPage *idx_page,
                                std::string idx_page_key,
                                uint32_t page_id,
                                uint32_t file_page_id,
                                bool elevate)
{
    // Flushes the built index page.
    WriteReq *io_req = GetWriteReq();
    io_req->page_.emplace<0>(idx_page);
    AllocatePage(io_req, page_id, file_page_id);

    storage_manager->Write(io_req);

    // The index page is linked to the parent.
    if (elevate)
    {
        assert(stack_.size() >= 1);
        if (stack_.size() == 1)
        {
            stack_.emplace(stack_.begin(),
                           std::make_unique<IndexStackEntry>(
                               nullptr, idx_page_manager_->GetComparator()));
        }

        IndexStackEntry *parent_entry = stack_[stack_.size() - 2].get();
        parent_entry->changes_.emplace_back(
            std::move(idx_page_key), io_req->page_id_, WriteOp::Upsert);
    }
}

void WriteTask::FinishDataPage(std::string_view page_view,
                               std::string page_key,
                               uint32_t page_id,
                               uint32_t file_page_id)
{
    uint32_t new_page_id = page_id == UINT32_MAX ? mapper_->GetPage() : page_id;
    DataPage new_page = AllocDataPage(new_page_id);
    memcpy(new_page.PagePtr(), page_view.data(), page_view.size());

    if (page_id == UINT32_MAX)
    {
        // This is a new data page that does not exist in the tree and has a new
        // page Id.
        LeafLinkInsert(std::move(new_page));

        // This is a new page that does not exist in the parent index page.
        // Elevates to the parent index page.
        assert(stack_.back()->changes_.empty() ||
               stack_.back()->changes_.back().key_ < page_key);
        stack_.back()->changes_.emplace_back(
            std::move(page_key), new_page_id, WriteOp::Upsert);
    }
    else
    {
        // This is an existing data page with updated content.
        LeafLinkUpdate(std::move(new_page), file_page_id);
    }
}

std::string_view WriteTask::LeftBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string_view idx_key = idx_iter.Key();
        if (!idx_key.empty())
        {
            return idx_key;
        }
        ++stack_it;
    }

    // An empty string for left bound means negative infinity.
    return std::string_view{};
}

std::string WriteTask::RightBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string next_key = idx_iter.PeekNextKey();
        if (!next_key.empty())
        {
            return next_key;
        }
        ++stack_it;
    }

    // An empty string for right bound means positive infinity.
    return std::string{};
}

void WriteTask::ApplyOnePage(size_t &cidx)
{
    assert(!stack_.empty());

    DataPage *base_page = nullptr;
    std::string_view page_left_bound{};
    std::string page_right_key;
    std::string_view page_right_bound{};

    if (stack_.back()->idx_page_iter_.PageId() != UINT32_MAX)
    {
        assert(stack_.back()->idx_page_iter_.PageId() == data_page_.PageId());
        base_page = &data_page_;
        page_left_bound = LeftBound(true);
        page_right_key = RightBound(true);
        page_right_bound = {page_right_key.data(), page_right_key.size()};
    }

    const Comparator *cmp = kv_options.comparator_;
    DataPageIter base_page_iter{base_page, cmp};
    bool is_base_iter_valid = false;
    AdvanceDataPageIter(base_page_iter, is_base_iter_valid);

    data_page_builder_.Reset();

    assert(cidx < data_.size());
    std::string_view change_key = {data_[cidx].key_.data(),
                                   data_[cidx].key_.size()};
    assert(cmp->Compare(page_left_bound, change_key) <= 0);
    assert(page_right_bound.empty() ||
           cmp->Compare(page_left_bound, page_right_bound) < 0);

    auto change_it = data_.begin() + cidx;
    auto change_end_it = std::lower_bound(
        change_it,
        data_.end(),
        page_right_bound,
        [&](const WriteDataEntry &change_item, std::string_view key)
        {
            if (key.empty())
            {
                // An empty-string right bound represents positive infinity.
                return true;
            }

            std::string_view ckey{change_item.key_.data(),
                                  change_item.key_.size()};
            return cmp->Compare(ckey, key) < 0;
        });

    std::string prev_key;
    std::string_view page_key = stack_.back()->idx_page_iter_.Key();
    std::string curr_page_key{page_key.data(), page_key.size()};

    uint32_t page_id = UINT32_MAX;
    uint32_t file_page_id = UINT32_MAX;
    if (base_page != nullptr)
    {
        page_id = base_page->PageId();
        file_page_id = ToFilePage(page_id);
        assert(page_key <= base_page_iter.Key());
    }

    auto add_to_page =
        [&](std::string_view key, std::string_view val, uint64_t ts)
    {
        bool success = data_page_builder_.Add(key, val, ts);
        if (!success)
        {
            // Finishes the current page.
            std::string_view page_view = data_page_builder_.Finish();
            FinishDataPage(
                page_view, std::move(curr_page_key), page_id, file_page_id);
            // Starts a new page.
            curr_page_key = cmp->FindShortestSeparator(
                {prev_key.data(), prev_key.size()}, key);
            assert(!prev_key.empty() && prev_key < curr_page_key);
            data_page_builder_.Reset();
            success = data_page_builder_.Add(key, val, ts);
            // Doesn't support a single key-value pair spanning more than one
            // page for now.
            assert(success);
            page_id = UINT32_MAX;
            file_page_id = UINT32_MAX;
        }
        assert(curr_page_key <= key);
        prev_key = key;
    };

    while (is_base_iter_valid && change_it != change_end_it)
    {
        std::string_view base_key = base_page_iter.Key();
        std::string_view base_val = base_page_iter.Value();
        uint64_t base_ts = base_page_iter.Timestamp();

        change_key = {change_it->key_.data(), change_it->key_.size()};
        std::string_view change_val = {change_it->val_.data(),
                                       change_it->val_.size()};
        uint64_t change_ts = change_it->timestamp_;

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        std::string_view new_val;
        uint64_t new_ts;
        AdvanceType adv_type;

        int cmp_ret = cmp->Compare(base_key, change_key);
        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_val = base_val;
            new_ts = base_ts;
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (change_ts > base_ts)
            {
                if (change_it->op_ == WriteOp::Delete)
                {
                    new_key = std::string_view{};
                }
                else
                {
                    new_key = change_key;
                    new_val = change_val;
                    new_ts = change_ts;
                }
            }
            else
            {
                new_key = base_key;
                new_val = base_val;
                new_ts = base_ts;
            }
        }
        else
        {
            adv_type = AdvanceType::Changes;
            if (change_it->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
            }
            else
            {
                new_key = change_key;
                new_val = change_val;
                new_ts = change_ts;
            }
        }

        if (!new_key.empty())
        {
            add_to_page(new_key, new_val, new_ts);
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            ++change_it;
            break;
        default:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            ++change_it;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        std::string_view new_key = base_page_iter.Key();
        std::string_view new_val = base_page_iter.Value();
        uint64_t new_ts = base_page_iter.Timestamp();
        add_to_page(new_key, new_val, new_ts);
        AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
    }

    while (change_it != change_end_it)
    {
        if (change_it->op_ != WriteOp::Delete)
        {
            std::string_view new_key{change_it->key_.data(),
                                     change_it->key_.size()};
            std::string_view new_val{change_it->val_.data(),
                                     change_it->val_.size()};
            uint64_t new_ts = change_it->timestamp_;
            add_to_page(new_key, new_val, new_ts);
        }
        ++change_it;
    }

    if (data_page_builder_.IsEmpty())
    {
        if (base_page)
        {
            LeafLinkDelete();
            FreePage(data_page_.PageId());
            assert(stack_.back()->changes_.empty() ||
                   stack_.back()->changes_.back().key_ < curr_page_key);
            stack_.back()->changes_.emplace_back(
                std::move(curr_page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        std::string_view page_view = data_page_builder_.Finish();
        FinishDataPage(
            page_view, std::move(curr_page_key), page_id, file_page_id);
    }
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(leaf_triple_[2]);

    cidx = cidx + std::distance(data_.begin() + cidx, change_end_it);
}

void WriteTask::AdvanceDataPageIter(DataPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

void WriteTask::AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

void WriteTask::RecycleWriteReq(WriteReq *req)
{
    WriteReq *first = free_req_head_.next_;
    free_req_head_.next_ = req;
    req->next_ = first;
    ++free_req_cnt_;
}

WriteReq *WriteTask::GetWriteReq()
{
    WriteReq *first = free_req_head_.next_;
    while (first == nullptr)
    {
        assert(free_req_cnt_ == 0);
        status_ = TaskStatus::BlockedForOne;
        Yield();
        first = free_req_head_.next_;
    }

    free_req_head_.next_ = first->next_;
    first->next_ = nullptr;
    first->task_ = this;
    first->tbl_ident_ = &tbl_ident_;
    --free_req_cnt_;
    return first;
}

void WriteTask::FinishIo(WriteReq *req)
{
    if (req->page_.index() == 0)
    {
        MemIndexPage *page = std::get<0>(req->page_);
        idx_page_manager_->FinishIo(
            new_mapping_, page, req->page_id_, req->file_page_id_);
    }
    else
    {
        FreeDataPage(std::move(std::get<1>(req->page_)));
    }

    RecycleWriteReq(req);
}

void WriteTask::Apply()
{
    CowRootMeta cow_meta = idx_page_manager_->MakeCowRoot(tbl_ident_);
    MemIndexPage *old_root = cow_meta.root_;
    mapper_ = std::move(cow_meta.new_mapper_);
    new_mapping_ = mapper_->GetMapping();
    old_mapping_ = std::move(cow_meta.old_mapping_);

    stack_.emplace_back(std::make_unique<IndexStackEntry>(
        old_root, idx_page_manager_->GetComparator()));

    if (old_root != nullptr)
    {
        old_root->Pin();
    }

    size_t cidx = 0;
    while (cidx < data_.size())
    {
        std::string_view batch_start_key = {data_[cidx].key_.data(),
                                            data_[cidx].key_.size()};
        if (stack_.size() > 1)
        {
            SeekStack(batch_start_key);
        }
        Seek(batch_start_key);
        ApplyOnePage(cidx);
    }
    // Flush all dirty leaf data pages in leaf_triple_ .
    assert(TripleElement(2) == nullptr);
    ShiftLeafLink();
    ShiftLeafLink();

    assert(!stack_.empty());
    MemIndexPage *new_root = nullptr;
    while (!stack_.empty())
    {
        new_root = Pop();
    }

    idx_page_manager_->UpdateRoot(tbl_ident_, new_root, std::move(mapper_));
    old_mapping_ = nullptr;
}

void WriteTask::SeekStack(std::string_view search_key)
{
    const Comparator *cmp = idx_page_manager_->GetComparator();

    auto entry_contains = [](std::string_view start,
                             std::string_view end,
                             std::string_view search_key,
                             const Comparator *cmp)
    {
        return (start.empty() || cmp->Compare(search_key, start) >= 0) &&
               (end.empty() || cmp->Compare(search_key, end) < 0);
    };

    // The bottom index entry (i.e., the tree root) ranges from negative
    // infinity to positive infinity.
    while (stack_.size() > 1)
    {
        IndexPageIter &idx_iter = stack_.back()->idx_page_iter_;

        if (idx_iter.HasNext())
        {
            idx_iter.Next();
            std::string_view idx_entry_start = idx_iter.Key();
            std::string idx_entry_end = RightBound(false);

            if (entry_contains(idx_entry_start, idx_entry_end, search_key, cmp))
            {
                break;
            }
            else
            {
                Pop();
            }
        }
        else
        {
            Pop();
        }
    }
}

void WriteTask::Seek(std::string_view key)
{
    if (stack_.back()->idx_page_ == nullptr)
    {
        stack_.back()->is_leaf_index_ = true;
        return;
    }

    while (true)
    {
        IndexStackEntry *idx_entry = stack_.back().get();
        IndexPageIter &idx_iter = idx_entry->idx_page_iter_;
        idx_iter.Seek(key);
        uint32_t page_id = idx_iter.PageId();
        assert(page_id != UINT32_MAX);

        MemIndexPage *node = idx_entry->idx_page_;
        if (node->IsPointingToLeaf())
        {
            break;
        }
        assert(!stack_.back()->is_leaf_index_);
        node = idx_page_manager_->FindPage(new_mapping_, page_id);
        assert(node);
        node->Pin();
        stack_.emplace_back(std::make_unique<IndexStackEntry>(
            node, idx_page_manager_->GetComparator()));
    }

    assert(!stack_.empty());
    uint32_t page_id = stack_.back()->idx_page_iter_.PageId();
    assert(page_id != UINT32_MAX);
    // Now we are going to fetch a data page before execute ApplyOnePage.
    // But this page may already exists at leaf_triple_[1], because it may be
    // loaded by previous ApplyOnePage for linking purpose.
    if (TripleElement(1) && TripleElement(1)->PageId() == page_id)
    {
        // Fast path: leaf_triple_[1] is exactly the page we want. Just move it
        // to avoid a disk access.
        if (data_page_.PagePtr() != nullptr)
        {
            FreeDataPage(std::move(data_page_));
        }
        DataPage *pg = TripleElement(1);
        data_page_ = std::move(*pg);
    }
    else
    {
        data_page_.SetPageId(page_id);
        storage_manager->Read(data_page_.PagePtr(),
                              kv_options.data_page_size,
                              tbl_ident_,
                              ToFilePage(page_id));
    }
    assert(PageType(data_page_.PagePtr()[0]) == PageType::Data);

    stack_.back()->is_leaf_index_ = true;
}

void WriteTask::AllocatePage(WriteReq *req,
                             uint32_t page_id,
                             uint32_t file_page_id)
{
    if (page_id == UINT32_MAX)
    {
        req->page_id_ = mapper_->GetPage();
        req->file_page_id_ = mapper_->GetFilePage();
    }
    else
    {
        req->page_id_ = page_id;
        req->file_page_id_ = mapper_->GetFilePage();
        assert(file_page_id != UINT32_MAX);
        // The page is mapped to a new file page. The old file page will be
        // recycled. However, the old file page shall only be recycled when the
        // old mapping snapshot is destructed, i.e., no one is using the old
        // mapping.
        old_mapping_->AddFreeFilePage(file_page_id);
    }
    new_mapping_->UpdateMapping(req->page_id_, req->file_page_id_);
}

void WriteTask::FreePage(uint32_t page_id)
{
    assert(page_id != UINT32_MAX);
    MemIndexPage *ptr = new_mapping_->GetSwizzlingPointer(page_id);
    uint32_t file_page = ptr ? ptr->FilePageId() : ToFilePage(page_id);
    mapper_->FreePage(page_id);
    old_mapping_->AddFreeFilePage(file_page);
}

uint32_t WriteTask::ToFilePage(uint32_t page_id)
{
    assert(mapper_ != nullptr);
    return mapper_->GetMappingSnapshot()->ToFilePage(page_id);
}

DataPage WriteTask::AllocDataPage(uint32_t page_id)
{
    if (data_pages_pool_.empty())
    {
        return DataPage(page_id);
    }
    else
    {
        DataPage page = std::move(data_pages_pool_.back());
        data_pages_pool_.pop_back();
        page.SetPageId(page_id);
        return page;
    }
}

void WriteTask::FreeDataPage(DataPage &&page)
{
    data_pages_pool_.push_back(std::move(page));
}

inline DataPage *WriteTask::TripleElement(uint8_t idx)
{
    return leaf_triple_[idx].page.PagePtr() ? &leaf_triple_[idx].page : nullptr;
}

void WriteTask::LoadTripleElement(uint8_t idx, uint32_t page_id)
{
    if (TripleElement(idx))
    {
        return;
    }
    assert(page_id != UINT32_MAX);
    uint32_t file_page = ToFilePage(page_id);
    leaf_triple_[idx].old_file_page = file_page;
    leaf_triple_[idx].page = AllocDataPage(page_id);
    storage_manager->Read(TripleElement(idx)->PagePtr(),
                          kv_options.data_page_size,
                          tbl_ident_,
                          file_page);
    assert(kv_error_ == KvError::NoError);
}

void WriteTask::ShiftLeafLink()
{
    if (TripleElement(0))
    {
        uint32_t old_file_page = leaf_triple_[0].old_file_page;
        WriteReq *req = GetWriteReq();
        req->page_id_ = leaf_triple_[0].page.PageId();
        req->page_.emplace<1>(std::move(leaf_triple_[0].page));
        if (old_file_page == UINT32_MAX)
        {
            // insert page
            req->file_page_id_ = mapper_->GetFilePage();
            new_mapping_->UpdateMapping(req->page_id_, req->file_page_id_);
        }
        else
        {
            // update page
            AllocatePage(req, req->page_id_, old_file_page);
        }
        storage_manager->Write(req);
    }
    leaf_triple_[0] = std::move(leaf_triple_[1]);
}

void WriteTask::LeafLinkUpdate(DataPage &&page, uint32_t old_fp)
{
    if (TripleElement(1))
    {
        assert(TripleElement(1)->PageId() != data_page_.PageId());
        ShiftLeafLink();
    }
    if (TripleElement(0) &&
        TripleElement(0)->PageId() != data_page_.PrevPageId())
    {
        // leaf_triple_[0] is not the previously adjacent page of the
        // applying page.
        ShiftLeafLink();
    }

    leaf_triple_[1].page = std::move(page);
    leaf_triple_[1].old_file_page = old_fp;

    DataPage &new_elem = leaf_triple_[1].page;
    new_elem.SetNextPageId(data_page_.NextPageId());
    new_elem.SetPrevPageId(data_page_.PrevPageId());
    ShiftLeafLink();
}

void WriteTask::LeafLinkInsert(DataPage &&page)
{
    assert(!TripleElement(1));
    leaf_triple_[1].page = std::move(page);
    leaf_triple_[1].old_file_page = UINT32_MAX;

    DataPage &new_elem = leaf_triple_[1].page;
    if (TripleElement(0) == nullptr)
    {
        // Add first element into empty link list
        assert(stack_.back()->idx_page_iter_.PageId() == UINT32_MAX);
        new_elem.SetNextPageId(UINT32_MAX);
        new_elem.SetPrevPageId(UINT32_MAX);
        ShiftLeafLink();
        return;
    }
    DataPage *prev_page = TripleElement(0);
    assert(stack_.back()->idx_page_iter_.PageId() == UINT32_MAX ||
           prev_page->NextPageId() == data_page_.NextPageId());
    if (prev_page->NextPageId() != UINT32_MAX)
    {
        LoadTripleElement(2, prev_page->NextPageId());
        TripleElement(2)->SetPrevPageId(new_elem.PageId());
    }
    new_elem.SetPrevPageId(prev_page->PageId());
    new_elem.SetNextPageId(prev_page->NextPageId());
    prev_page->SetNextPageId(new_elem.PageId());
    ShiftLeafLink();
}

void WriteTask::LeafLinkDelete()
{
    if (TripleElement(1))
    {
        assert(TripleElement(1)->PageId() != data_page_.PageId());
        ShiftLeafLink();
    }
    if (TripleElement(0) &&
        TripleElement(0)->PageId() != data_page_.PrevPageId())
    {
        // leaf_triple_[0] is not the previously adjacent page of the
        // applying page.
        ShiftLeafLink();
    }

    if (data_page_.PrevPageId() != UINT32_MAX)
    {
        LoadTripleElement(0, data_page_.PrevPageId());
        TripleElement(0)->SetNextPageId(data_page_.NextPageId());
    }
    if (data_page_.NextPageId() != UINT32_MAX)
    {
        assert(!TripleElement(2));
        LoadTripleElement(2, data_page_.NextPageId());
        TripleElement(2)->SetPrevPageId(data_page_.PrevPageId());
    }
}
}  // namespace kvstore