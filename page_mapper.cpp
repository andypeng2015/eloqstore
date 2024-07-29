#include "page_mapper.h"

#include <cassert>

#include "global_variables.h"
#include "index_page_manager.h"

namespace kvstore
{
PageMapper::PageMapper(const TableIdent *tbl_ident)
{
    mapping_ = std::make_shared<MappingSnapshot>(tbl_ident);

    std::vector<uint64_t> &map = mapping_->mapping_tbl_;
    map.resize(kv_options.init_page_count);
    uint32_t page_id = map.size() - 1;
    for (auto rit = map.rbegin(); rit != map.rend(); ++rit, --page_id)
    {
        FreePage(page_id);
    }
}

PageMapper::PageMapper(const PageMapper &rhs)
    : free_page_head_(rhs.free_page_head_),
      free_file_pages_(rhs.free_file_pages_),
      min_file_page_id_(rhs.min_file_page_id_),
      max_file_page_id_(rhs.max_file_page_id_)
{
    mapping_ = std::make_shared<MappingSnapshot>(rhs.mapping_->tbl_ident_);
    mapping_->mapping_tbl_ = std::vector<uint64_t>(rhs.mapping_->mapping_tbl_);
}

void PageMapper::FreePage(uint32_t page_id)
{
    auto &map = Mapping();
    assert(page_id < map.size());
    map[page_id] = free_page_head_;
    free_page_head_ = page_id;
}

void PageMapper::FreeFilePages(std::vector<uint32_t> file_pages)
{
    free_file_pages_.insert(file_pages.begin(), file_pages.end());
}

uint32_t PageMapper::GetPage()
{
    auto &map = Mapping();
    if (free_page_head_ == UINT32_MAX)
    {
        map.emplace_back(UINT32_MAX);
        return map.size() - 1;
    }
    else
    {
        uint32_t free_page = free_page_head_;
        // The free page head points to the next free page.
        free_page_head_ = map[free_page];
        // Sets the free page's mapped file page to null.
        map[free_page] = UINT32_MAX;
        return free_page;
    }
}

uint32_t PageMapper::GetFilePage()
{
    uint32_t fp_id = GetFreeFilePage();

    if (fp_id < UINT32_MAX)
    {
        if (fp_id < min_file_page_id_)
        {
            min_file_page_id_ = fp_id;
        }
        else if (fp_id > max_file_page_id_)
        {
            max_file_page_id_ = fp_id;
        }
    }
    else
    {
        if (min_file_page_id_ <= max_file_page_id_)
        {
            if (min_file_page_id_ > 0)
            {
                fp_id = min_file_page_id_ - 1;
                min_file_page_id_ = fp_id;
            }
            else
            {
                fp_id = max_file_page_id_ + 1;
                max_file_page_id_ = fp_id;
            }
        }
        else
        {
            // There is no file page at all.
            min_file_page_id_ = 0;
            max_file_page_id_ = 0;
            fp_id = 0;
        }
    }

    return fp_id;
}

std::shared_ptr<MappingSnapshot> PageMapper::GetMappingSnapshot()
{
    return mapping_;
}

MappingSnapshot *PageMapper::GetMapping()
{
    return mapping_.get();
}

void PageMapper::FreeMappingSnapshot()
{
    mapping_ = nullptr;
}

std::vector<uint64_t> &PageMapper::Mapping()
{
    return mapping_->mapping_tbl_;
}

uint32_t PageMapper::GetFreeFilePage()
{
    auto &free_file_pages = free_file_pages_;
    if (free_file_pages.empty())
    {
        return UINT32_MAX;
    }
    else
    {
        auto it = free_file_pages.begin();
        uint32_t fp_id = *it;
        free_file_pages.erase(it);
        return fp_id;
    }
}

uint64_t PageMapper::EncodeFilePage(uint32_t file_page_id)
{
    return (file_page_id << 1) | 1;
}

uint32_t PageMapper::DecodeFilePage(uint64_t val)
{
    return val >> 1;
}

bool PageMapper::IsSwizzlingPointer(uint64_t val)
{
    return (val & 1) == 0;
}

MappingSnapshot::MappingSnapshot(const TableIdent *tbl) : tbl_ident_(tbl)
{
}

MappingSnapshot::~MappingSnapshot()
{
    idx_manager->FreeMappingSnapshot(this);
}

uint32_t MappingSnapshot::ToFilePage(uint32_t page_id)
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    assert((val & 1) == 1);
    return PageMapper::DecodeFilePage(val);
}

void MappingSnapshot::UpdateMapping(uint32_t page_id, uint32_t file_page_id)
{
    assert(page_id < mapping_tbl_.size());
    mapping_tbl_[page_id] = PageMapper::EncodeFilePage(file_page_id);
}

void MappingSnapshot::AddFreeFilePage(uint32_t file_page)
{
    to_free_file_pages_.emplace_back(file_page);
}

void MappingSnapshot::Unswizzling(MemIndexPage *page)
{
    uint32_t page_id = page->PageId();
    uint32_t file_page_id = page->FilePageId();

    if (page_id < mapping_tbl_.size() &&
        PageMapper::IsSwizzlingPointer(mapping_tbl_[page_id]) &&
        reinterpret_cast<MemIndexPage *>(mapping_tbl_[page_id]) == page)
    {
        mapping_tbl_[page_id] = PageMapper::EncodeFilePage(file_page_id);
    }
}

MemIndexPage *MappingSnapshot::GetSwizzlingPointer(uint32_t page_id)
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (PageMapper::IsSwizzlingPointer(val))
    {
        MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
        return idx_page;
    }
    else
    {
        return nullptr;
    }
}

void MappingSnapshot::AddSwizzling(uint32_t page_id, MemIndexPage *idx_page)
{
    assert(page_id < mapping_tbl_.size());

    uint64_t val = mapping_tbl_[page_id];
    if (PageMapper::IsSwizzlingPointer(val))
    {
        assert(reinterpret_cast<MemIndexPage *>(val) == idx_page);
    }
    else
    {
        assert(PageMapper::DecodeFilePage(val) == idx_page->FilePageId());
        mapping_tbl_[page_id] = reinterpret_cast<uint64_t>(idx_page);
    }
}
}  // namespace kvstore