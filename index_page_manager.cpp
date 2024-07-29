#include "index_page_manager.h"

#include <cstdint>

#include "global_variables.h"
#include "kv_options.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "storage_manager.h"

namespace kvstore
{
IndexPageManager::IndexPageManager(const KvOptions *opt,
                                   AsyncIoManager *io_manager)
    : options_(opt),
      read_reqs_(opt->index_page_read_queue),
      io_manager_(io_manager)
{
    active_head_.EnqueNext(&active_tail_);

    for (auto &req : read_reqs_)
    {
        RecycleReadReq(&req);
    }
}

IndexPageManager::~IndexPageManager()
{
    for (auto &[tbl, meta] : tbl_roots_)
    {
        // Destructs page mapper first, because destructing the mapping snapshot
        // needs to access the root table in the index page manager.
        meta.mapper_->FreeMappingSnapshot();
    }
}

const Comparator *IndexPageManager::GetComparator() const
{
    return options_->comparator_;
}

MemIndexPage *IndexPageManager::AllocIndexPage()
{
    MemIndexPage *next_free = free_head_.DequeNext();
    while (next_free == nullptr)
    {
        if (!IsFull())
        {
            auto &new_page =
                index_pages_.emplace_back(std::make_unique<MemIndexPage>());
            next_free = new_page.get();
        }
        else
        {
            bool success = Evict();
            if (!success)
            {
                // There is no page to evict because all pages are pinned.
                // Tasks trying to allocate new pages should rollback to unpin
                // pages in the task's traversal stack.
                return nullptr;
            }
            next_free = free_head_.DequeNext();
        }
    }
    assert(next_free->IsDetached());

    return next_free;
}

void IndexPageManager::EnqueuIndexPage(MemIndexPage *page)
{
    if (page->prev_ != nullptr)
    {
        assert(page->next_ != nullptr);
        page->Deque();
    }
    assert(page->prev_ == nullptr && page->next_ == nullptr);
    active_head_.EnqueNext(page);
}

bool IndexPageManager::IsFull() const
{
    return index_pages_.size() >= kv_options.index_buffer_pool_size;
}

std::pair<MemIndexPage *, PageMapper *> IndexPageManager::FindRoot(
    const TableIdent &tbl_ident)
{
    auto it = tbl_roots_.find(tbl_ident);
    if (it == tbl_roots_.end() || it->second.root_page_ == nullptr)
    {
        return {nullptr, nullptr};
    }
    else
    {
        RootMeta &meta = it->second;
        EnqueuIndexPage(meta.root_page_);
        return {meta.root_page_, meta.mapper_.get()};
    }
}

CowRootMeta IndexPageManager::MakeCowRoot(const TableIdent &tbl_ident)
{
    auto tbl_it = tbl_roots_.try_emplace(tbl_ident, nullptr, nullptr);
    RootMeta &meta = tbl_it.first->second;
    if (tbl_it.second)
    {
        const TableIdent *tbl = &tbl_it.first->first;
        std::unique_ptr<PageMapper> mapper = std::make_unique<PageMapper>(tbl);
        std::shared_ptr<MappingSnapshot> mapping = mapper->GetMappingSnapshot();
        meta.mapping_snapshots_.emplace(mapper->GetMapping());

        return {nullptr, std::move(mapper), std::move(mapping)};
    }
    else
    {
        // Makes a copy of the mapper.
        std::unique_ptr<PageMapper> new_mapper =
            std::make_unique<PageMapper>(*meta.mapper_);
        meta.mapping_snapshots_.emplace(new_mapper->GetMapping());

        return {meta.root_page_,
                std::move(new_mapper),
                meta.mapper_->GetMappingSnapshot()};
    }
}

void IndexPageManager::UpdateRoot(const TableIdent &tbl_ident,
                                  MemIndexPage *new_root,
                                  std::unique_ptr<PageMapper> new_mapper)
{
    MemIndexPage *old_root = nullptr;
    auto tbl_it = tbl_roots_.find(tbl_ident);
    if (tbl_it == tbl_roots_.end())
    {
        tbl_roots_.try_emplace(tbl_ident, new_root, std::move(new_mapper));
    }
    else
    {
        RootMeta &meta = tbl_it->second;
        old_root = meta.root_page_;
        meta.root_page_ = new_root;
        meta.mapper_ = std::move(new_mapper);
    }
}

MemIndexPage *IndexPageManager::FindPage(MappingSnapshot *mapping,
                                         uint32_t page_id)
{
    // First checks swizzling pointers.
    MemIndexPage *idx_page = mapping->GetSwizzlingPointer(page_id);
    while (idx_page == nullptr)
    {
        auto it = loading_zone_.find(page_id);
        if (it != loading_zone_.end())
        {
            // There is already a read request issuing an async read on the same
            // page. Waits for the request in the waiting queue.
            it->second->pending_tasks_.emplace_back(thd_task);
            thd_task->Yield();
            // When resumed, the read request should have loaded the page,
            // unless the page is evicted again.
            idx_page = mapping->GetSwizzlingPointer(page_id);
        }
        else
        {
            // This is the first request to load the page.
            ReadReq *read_req = GetFreeReadReq();
            assert(read_req != nullptr);
            auto it = loading_zone_.try_emplace(page_id, read_req);
            assert(it.second);
            ReadReq *req = it.first->second;

            MemIndexPage *new_page = AllocIndexPage();
            if (new_page == nullptr)
            {
                // No page can be found because all pages are pinned. Rollback
                // the current task to unpin pages and avoid a deadlock.

                for (auto &task : req->pending_tasks_)
                {
                    // This task is about to rollback. Resumes other tasks
                    // waiting for it. Note: Resume() re-schedules the task to
                    // run, but does not run in-place.
                    task->Resume();
                }
                req->pending_tasks_.clear();
                RecycleReadReq(req);
                loading_zone_.erase(it.first);

                thd_task->Rollback();
                // Rollback() returns the control. The task should reset the
                // stack and restart and never reach this point after
                // Rollback().
                assert("A rollback task should not reach this point.");
            }

            // Read the page async.
            uint32_t file_page_id = mapping->ToFilePage(page_id);
            storage_manager->Read(new_page->PagePtr(),
                                  kv_options.index_page_size,
                                  *mapping->tbl_ident_,
                                  file_page_id);

            // The page has been loaded.
            if (thd_task->kv_error_ != KvError::NoError)
            {
            }

            FinishIo(mapping, new_page, page_id, file_page_id);

            for (auto &task : read_req->pending_tasks_)
            {
                task->Resume();
            }

            loading_zone_.erase(it.first);
            read_req->pending_tasks_.clear();
            RecycleReadReq(read_req);

            return new_page;
        }
    }
    EnqueuIndexPage(idx_page);

    return idx_page;
}

void IndexPageManager::FreeMappingSnapshot(MappingSnapshot *mapping)
{
    const TableIdent &tbl = *mapping->tbl_ident_;
    auto tbl_it = tbl_roots_.find(tbl);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;

    // Puts back file pages freed in this mapping snapshot. The page mapper
    // is null, when the process is shutting down and the index page manager
    // is being desturcted.
    if (meta.root_page_ != nullptr)
    {
        assert(meta.mapper_ != nullptr);
        meta.mapper_->FreeFilePages(std::move(mapping->to_free_file_pages_));
    }
    else
    {
        // The root is evicted before the old mapping snapshot is destructed.
        // Needs to bring back the root to update free file pages.
    }

    meta.mapping_snapshots_.erase(mapping);

    EvictRootIfEmpty(tbl_it);
}

void IndexPageManager::Unswizzling(MemIndexPage *page)
{
    auto tbl_it = tbl_roots_.find(*page->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());

    auto &mappings = tbl_it->second.mapping_snapshots_;
    for (auto &mapping : mappings)
    {
        mapping->Unswizzling(page);
    }
}

bool IndexPageManager::Evict()
{
    MemIndexPage *node = &active_tail_;

    do
    {
        while (node->prev_->IsPinned() && node->prev_ != &active_head_)
        {
            node = node->prev_;
        }

        // Has reached the head of the active list. Eviction failed.
        if (node->prev_ == &active_head_)
        {
            return false;
        }

        node = node->prev_;
        RecyclePage(node);
    } while (free_head_.next_ == nullptr);

    return true;
}

void IndexPageManager::EvictRootIfEmpty(
    std::unordered_map<TableIdent, RootMeta>::iterator root_it)
{
    RootMeta &meta = root_it->second;
    if (meta.root_page_ == nullptr && meta.mapping_snapshots_.empty() &&
        meta.ref_cnt_ == 0)
    {
        tbl_roots_.erase(root_it);
    }
}

bool IndexPageManager::RecyclePage(MemIndexPage *page)
{
    if (page->IsPinned())
    {
        return false;
    }

    // Removes the page from the active list.
    page->Deque();

    assert(page->page_id_ != UINT32_MAX);
    assert(page->file_page_id_ != UINT32_MAX);

    // Unswizzling the page pointer in all mapping snapshots.
    auto tbl_it = tbl_roots_.find(*page->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());
    RootMeta &meta = tbl_it->second;
    auto &mappings = tbl_it->second.mapping_snapshots_;
    for (auto &mapping : mappings)
    {
        mapping->Unswizzling(page);
    }

    // If the recycled page is the root, removes the entry from the root table.
    if (meta.root_page_ == page)
    {
        // To evict the root entry, first frees the root's mapping snapshot.
        meta.mapper_->FreeMappingSnapshot();
        meta.root_page_ = nullptr;
        meta.mapper_ = nullptr;

        if (meta.root_page_ == nullptr && meta.mapping_snapshots_.empty())
        {
            tbl_roots_.erase(tbl_it);
        }
    }

    assert(meta.ref_cnt_ > 0);
    --meta.ref_cnt_;

    EvictRootIfEmpty(tbl_it);

    page->page_id_ = UINT32_MAX;
    page->file_page_id_ = UINT32_MAX;
    page->tbl_ident_ = nullptr;

    free_head_.EnqueNext(page);

    return true;
}

void IndexPageManager::FinishIo(MappingSnapshot *mapping,
                                MemIndexPage *idx_page,
                                uint32_t page_id,
                                uint32_t file_page_id)
{
    idx_page->page_id_ = page_id;
    idx_page->file_page_id_ = file_page_id;
    idx_page->tbl_ident_ = mapping->tbl_ident_;
    mapping->AddSwizzling(page_id, idx_page);

    auto tbl_it = tbl_roots_.find(*mapping->tbl_ident_);
    assert(tbl_it != tbl_roots_.end());
    ++tbl_it->second.ref_cnt_;

    assert(idx_page->IsDetached());
    EnqueuIndexPage(idx_page);
}

void IndexPageManager::RecycleReadReq(ReadReq *entry)
{
    ReadReq *first = free_read_head_.next_;
    free_read_head_.next_ = entry;
    entry->next_ = first;

    if (waiting_zone_.Size() > 0)
    {
        KvTask *task = waiting_zone_.Peek();
        waiting_zone_.Dequeue();
        task->Resume();
    }
}

IndexPageManager::ReadReq *IndexPageManager::GetFreeReadReq()
{
    ReadReq *first = free_read_head_.next_;
    while (first == nullptr)
    {
        waiting_zone_.Enqueue(thd_task);
        thd_task->Yield();
        first = free_read_head_.next_;
    }

    free_read_head_.next_ = first->next_;
    first->next_ = nullptr;
    return first;
}
}  // namespace kvstore