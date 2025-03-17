#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "coding.h"
#include "crc32.h"

namespace kvstore
{
static uint16_t const page_crc_offset = 0;
static uint16_t const page_type_offset = page_crc_offset + sizeof(uint32_t);

enum struct PageType : uint8_t
{
    NonLeafIndex = 0,
    LeafIndex,
    Data,
    Deleted = 255
};

inline static PageType TypeOfPage(const char *p)
{
    return static_cast<PageType>(p[page_type_offset]);
}

inline static void SetPageType(char *p, PageType t)
{
    p[page_type_offset] = static_cast<char>(t);
}

inline static uint32_t Crc32OfPage(const char *p)
{
    return crc32::Unmask(DecodeFixed32(p + page_crc_offset));
}

inline static void SetPageCrc32(char *p, uint16_t pgsz)
{
    uint32_t crc = crc32::Value(p + page_type_offset, pgsz - page_type_offset);
    EncodeFixed32(p + page_crc_offset, crc32::Mask(crc));
}

inline static bool ValidatePageCrc32(char *p, uint16_t pgsz)
{
    uint32_t crc = crc32::Value(p + page_type_offset, pgsz - page_type_offset);
    return crc == Crc32OfPage(p);
}

inline static size_t page_align = sysconf(_SC_PAGESIZE);

using Page = std::unique_ptr<char, decltype(&std::free)>;

inline Page alloc_page(uint16_t page_size)
{
    char *p = (char *) std::aligned_alloc(page_align, page_size);
    assert(p);
    return Page(p, std::free);
}

class PagePool
{
public:
    PagePool(uint16_t page_size) : page_size_(page_size) {};
    Page Allocate()
    {
        if (pages_.empty())
        {
            return alloc_page(page_size_);
        }
        else
        {
            Page page = std::move(pages_.back());
            pages_.pop_back();
            return page;
        }
    }
    void Free(Page page)
    {
        pages_.emplace_back(std::move(page));
    }

private:
    const uint16_t page_size_;
    std::vector<Page> pages_;
};

inline thread_local PagePool *page_pool;
}  // namespace kvstore