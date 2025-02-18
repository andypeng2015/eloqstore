#include "page_mapper.h"

#include <cassert>
#include <cstdint>

#include "coding.h"
#include "index_page_manager.h"

namespace kvstore
{
// This is used for replay when recovering from WAL.
PageMapper::PageMapper()
{
    mapping_ = std::make_shared<MappingSnapshot>(nullptr, nullptr);
}

PageMapper::PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident)
{
    mapping_ = std::make_shared<MappingSnapshot>(idx_mgr, tbl_ident);
    InitPages(idx_mgr->Options()->init_page_count);
}

PageMapper::PageMapper(const PageMapper &rhs)
    : free_page_head_(rhs.free_page_head_),
      free_file_pages_(rhs.free_file_pages_),
      min_file_page_id_(rhs.min_file_page_id_),
      max_file_page_id_(rhs.max_file_page_id_)
{
    mapping_ = std::make_shared<MappingSnapshot>(rhs.mapping_->idx_mgr_,
                                                 rhs.mapping_->tbl_ident_);
    mapping_->mapping_tbl_ = std::vector<uint64_t>(rhs.mapping_->mapping_tbl_);
}

void PageMapper::InitPages(uint32_t page_count)
{
    std::vector<uint64_t> &map = mapping_->mapping_tbl_;
    assert(map.empty() && free_page_head_ == UINT32_MAX);
    map.resize(page_count);
    uint32_t page_id = map.size() - 1;
    for (auto rit = map.rbegin(); rit != map.rend(); ++rit, --page_id)
    {
        FreePage(page_id);
    }
}

void PageMapper::FreePage(uint32_t page_id)
{
    auto &map = Mapping();
    assert(page_id < map.size());
    map[page_id] = free_page_head_ == UINT32_MAX
                       ? UINT32_MAX
                       : (free_page_head_ << MAPPING_BITS) | MAPPING_FREE;
    free_page_head_ = page_id;
}

void PageMapper::FreeFilePages(std::vector<uint32_t> file_pages)
{
    free_file_pages_.insert(file_pages.begin(), file_pages.end());
}

void PageMapper::FreeFilePage(uint32_t file_page)
{
    free_file_pages_.insert(file_page);
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
        free_page_head_ = mapping_->GetNextFree(free_page);
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
        assert(fp_id >= min_file_page_id_ && fp_id <= max_file_page_id_);
    }
    else
    {
        fp_id = ExpFilePage();
    }

    return fp_id;
}

uint32_t PageMapper::ExpFilePage()
{
    uint32_t fp_id;
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

uint32_t PageMapper::UseCount()
{
    return mapping_ == nullptr ? 0 : mapping_.use_count();
}

void PageMapper::FreeMappingSnapshot()
{
    mapping_ = nullptr;
}

std::vector<uint64_t> &PageMapper::Mapping()
{
    return mapping_->mapping_tbl_;
}

void PageMapper::UpdateMapping(uint32_t page_id, uint32_t file_page_id)
{
    assert(page_id < mapping_->mapping_tbl_.size());
    mapping_->mapping_tbl_[page_id] = PageMapper::EncodeFilePage(file_page_id);
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

bool PageMapper::GetFreeFilePage(uint32_t fp_id)
{
    return free_file_pages_.erase(fp_id);
}

uint64_t PageMapper::EncodeFilePage(uint32_t file_page_id)
{
    return (file_page_id << MAPPING_BITS) | MAPPING_PHY_ID;
}

uint32_t PageMapper::DecodeFilePage(uint64_t val)
{
    return val >> MAPPING_BITS;
}

bool PageMapper::IsSwizzlingPointer(uint64_t val)
{
    return (val & MAPPING_MASK) == 0;
}

bool PageMapper::DequeFree(uint32_t page_id)
{
    auto &map = Mapping();
    uint32_t prev = free_page_head_;
    if (prev == UINT32_MAX)
    {
        return false;
    }
    if (prev == page_id)
    {
        uint32_t pgid = GetPage();
        assert(pgid == page_id);
        return true;
    }
    uint32_t cur = mapping_->GetNextFree(prev);
    while (cur != UINT32_MAX && cur != page_id)
    {
        prev = cur;
        cur = mapping_->GetNextFree(cur);
    }
    if (cur == UINT32_MAX)
    {
        return false;
    }
    assert(cur == page_id);
    map[prev] = map[cur];
    map[cur] = UINT32_MAX;
    return true;
}

uint32_t PageMapper::PeekFree()
{
    return free_page_head_;
}

void PageMapper::Serialize(std::string &dst) const
{
    mapping_->Serialize(dst);
    PutVarint32(&dst, free_page_head_);

    PutVarint32(&dst, free_file_pages_.size());
    for (uint32_t id : free_file_pages_)
    {
        PutVarint32(&dst, id);
    }
    PutVarint32(&dst, min_file_page_id_);
    PutVarint32(&dst, max_file_page_id_);
}

std::string_view PageMapper::Deserialize(std::string_view src)
{
    uint32_t sz, fp_id;

    GetVarint32(&src, &sz);
    mapping_->mapping_tbl_.resize(sz);
    for (uint32_t i = 0; i < sz; i++)
    {
        GetVarint32(&src, &fp_id);
        mapping_->mapping_tbl_[i] = fp_id;
    }
    GetVarint32(&src, &free_page_head_);

    GetVarint32(&src, &sz);
    for (uint32_t i = 0; i < sz; i++)
    {
        GetVarint32(&src, &fp_id);
        free_file_pages_.insert(fp_id);
    }
    GetVarint32(&src, &min_file_page_id_);
    GetVarint32(&src, &max_file_page_id_);
    return src;
}

bool PageMapper::EqualTo(const PageMapper &rhs) const
{
    return free_page_head_ == rhs.free_page_head_ &&
           free_file_pages_ == rhs.free_file_pages_ &&
           min_file_page_id_ == rhs.min_file_page_id_ &&
           max_file_page_id_ == rhs.max_file_page_id_ &&
           free_file_pages_ == rhs.free_file_pages_ &&
           mapping_->mapping_tbl_ == rhs.mapping_->mapping_tbl_ &&
           mapping_->to_free_file_pages_ == rhs.mapping_->to_free_file_pages_;
}

MappingSnapshot::MappingSnapshot(IndexPageManager *idx_mgr,
                                 const TableIdent *tbl)
    : idx_mgr_(idx_mgr), tbl_ident_(tbl)
{
}

MappingSnapshot::~MappingSnapshot()
{
    if (idx_mgr_)
    {
        idx_mgr_->FreeMappingSnapshot(this);
    }
}

uint32_t MappingSnapshot::ToFilePage(uint32_t page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    assert(val & MAPPING_PHY_ID);
    return PageMapper::DecodeFilePage(val);
}

uint32_t MappingSnapshot::GetFilePage(uint32_t page_id) const
{
    assert((mapping_tbl_[page_id] & MAPPING_FREE) == 0);
    MemIndexPage *p = GetSwizzlingPointer(page_id);
    return p ? p->FilePageId() : ToFilePage(page_id);
}

uint32_t MappingSnapshot::GetNextFree(uint32_t page_id) const
{
    assert(page_id < mapping_tbl_.size());
    uint64_t val = mapping_tbl_[page_id];
    if (val == UINT32_MAX)
    {
        return UINT32_MAX;
    }
    assert(val & MAPPING_FREE);
    return val >> MAPPING_BITS;
}

void MappingSnapshot::AddFreeFilePage(uint32_t file_page)
{
    to_free_file_pages_.emplace_back(file_page);
}

void MappingSnapshot::Unswizzling(MemIndexPage *page)
{
    uint32_t page_id = page->PageId();
    uint32_t file_page_id = page->FilePageId();
    assert((mapping_tbl_[page_id] & MAPPING_FREE) == 0);

    if (page_id < mapping_tbl_.size() &&
        PageMapper::IsSwizzlingPointer(mapping_tbl_[page_id]) &&
        reinterpret_cast<MemIndexPage *>(mapping_tbl_[page_id]) == page)
    {
        mapping_tbl_[page_id] = PageMapper::EncodeFilePage(file_page_id);
    }
}

MemIndexPage *MappingSnapshot::GetSwizzlingPointer(uint32_t page_id) const
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

void MappingSnapshot::Serialize(std::string &dst) const
{
    PutVarint32(&dst, mapping_tbl_.size());
    for (uint32_t i = 0; i < mapping_tbl_.size(); i++)
    {
        uint32_t val = mapping_tbl_[i];
        MemIndexPage *p = GetSwizzlingPointer(i);
        if (p)
        {
            val = PageMapper::EncodeFilePage(p->FilePageId());
        }
        PutVarint32(&dst, val);
    }
}

}  // namespace kvstore