#pragma once

#include <cstdint>
#include <span>
#include <string>

#include "comparator.h"
#include "kv_options.h"
#include "page.h"

namespace kvstore
{
enum class ValLenBit : uint8_t
{
    Overflow = 0,
    Reserved1,
    Reserved2,
    Reserved3,
    BitsCount
};

class DataPage
{
public:
    DataPage() = default;
    DataPage(uint32_t page_id, uint32_t page_size = 0);
    DataPage(uint32_t page_id, Page page)
        : page_id_(page_id), page_(std::move(page)) {};
    DataPage(const DataPage &) = delete;
    DataPage(DataPage &&rhs);
    DataPage &operator=(DataPage &&);
    ~DataPage();

    static uint16_t const page_size_offset = page_type_offset + sizeof(uint8_t);
    static uint16_t const prev_page_offset =
        page_size_offset + sizeof(uint16_t);
    static uint16_t const next_page_offset =
        prev_page_offset + sizeof(uint32_t);
    static uint16_t const content_offset = next_page_offset + sizeof(uint32_t);

    bool IsEmpty() const;
    uint16_t ContentLength() const;
    uint16_t RestartNum() const;
    uint32_t PrevPageId() const;
    uint32_t NextPageId() const;
    void SetPrevPageId(uint32_t page_id);
    void SetNextPageId(uint32_t page_id);
    void SetPageId(uint32_t page_id);
    uint32_t PageId() const;
    char *PagePtr() const;
    Page GetPtr();
    void SetPtr(Page ptr);
    void Clear();

private:
    uint32_t page_id_{UINT32_MAX};
    Page page_{nullptr, std::free};
};

std::ostream &operator<<(std::ostream &out, DataPage const &page);

class DataPageIter
{
public:
    DataPageIter() = delete;
    DataPageIter(const DataPage *data_page, const KvOptions *options);

    void Reset(const DataPage *data_page, uint32_t size);
    void Reset();
    std::string_view Key() const;
    std::string_view Value() const;
    bool IsOverflow() const;
    uint64_t Timestamp() const;

    bool HasNext() const;
    bool Next();
    void Seek(std::string_view search_key);

private:
    uint16_t RestartOffset(uint16_t restart_idx) const;
    void SeekToRestart(uint16_t restart_idx);
    bool ParseNextKey();
    void Invalidate();
    static const char *DecodeEntry(const char *p,
                                   const char *limit,
                                   uint32_t *shared,
                                   uint32_t *non_shared,
                                   uint32_t *value_length,
                                   bool *overflow);

    const Comparator *const cmp_;
    std::string_view page_;
    uint16_t restart_num_;
    uint16_t restart_offset_;

    uint16_t curr_offset_{DataPage::content_offset};
    uint16_t curr_restart_idx_{0};

    std::string key_;
    std::string_view value_;
    bool overflow_;
    uint64_t timestamp_;
};

/**
 * @brief The overflow page is used to store the overflow value that can not fit
 * in a DataPage.
 * Format:
 * +---------------+-----------+----------------+-------+
 * | checksum (8B) | type (1B) | value len (2B) | value |
 * +---------------+-----------+----------------+-------+
 * +----------------------+-------------------------+
 * | pointers (N*4 Bytes) | number of pointers (1B) | <End>
 * +----------------------+-------------------------+
 * The pointers are stored at the end of the page.
 */
class OverflowPage
{
public:
    OverflowPage() = default;
    OverflowPage(uint32_t page_id, Page page);
    OverflowPage(uint32_t page_id,
                 const KvOptions *opts,
                 std::string_view val,
                 std::span<uint32_t> pointers = {});
    OverflowPage(const OverflowPage &) = delete;
    OverflowPage(OverflowPage &&rhs);
    ~OverflowPage();
    void Clear();
    void SetPageId(uint32_t page_id);
    uint32_t PageId() const;
    char *PagePtr() const;
    uint16_t ValueSize() const;
    std::string_view GetValue() const;
    uint8_t NumPointers(const KvOptions *options) const;
    std::string_view GetEncodedPointers(const KvOptions *options) const;

    /**
     * @brief Calculate the capacity of an overflow page.
     * @param options The options of the KV store.
     * @param end Whether the page is the end page of a overflow group.
     */
    static uint16_t Capacity(const KvOptions *options, bool end);

    static const uint16_t page_size_offset = page_type_offset + sizeof(uint8_t);
    static const uint16_t value_offset = page_size_offset + sizeof(uint16_t);
    static const uint16_t header_size = value_offset;

private:
    uint32_t page_id_{UINT32_MAX};
    Page page_{nullptr, std::free};
};
}  // namespace kvstore