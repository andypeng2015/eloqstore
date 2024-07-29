#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <vector>

#include "table_ident.h"

namespace kvstore
{
class MemIndexPage;

struct MappingSnapshot
{
    MappingSnapshot(const TableIdent *tbl);
    ~MappingSnapshot();

    uint32_t ToFilePage(uint32_t page_id);

    void UpdateMapping(uint32_t page_id, uint32_t file_page_id);

    void AddFreeFilePage(uint32_t file_page);

    /**
     * @brief Replaces the swizzling pointer with the file page Id.
     *
     * @param page
     */
    void Unswizzling(MemIndexPage *page);

    MemIndexPage *GetSwizzlingPointer(uint32_t page_id);

    void AddSwizzling(uint32_t page_id, MemIndexPage *idx_page);

    const TableIdent *tbl_ident_;

    std::vector<uint64_t> mapping_tbl_;
    /**
     * @brief A list of file pages to be freed in this mapping snapshot.
     * To-be-freed file pages cannot be put back for re-use if someone is using
     * this snapshot.
     *
     */
    std::vector<uint32_t> to_free_file_pages_;
};

class PageMapper
{
public:
    PageMapper(const TableIdent *tbl_ident);
    PageMapper(const PageMapper &rhs);

    uint32_t GetPage();
    uint32_t GetFilePage();
    void FreePage(uint32_t page_id);
    void FreeFilePages(std::vector<uint32_t> file_pages);

    std::shared_ptr<MappingSnapshot> GetMappingSnapshot();
    MappingSnapshot *GetMapping();

    void FreeMappingSnapshot();

    static uint64_t EncodeFilePage(uint32_t file_page_id);
    static uint32_t DecodeFilePage(uint64_t val);
    static bool IsSwizzlingPointer(uint64_t val);

private:
    std::vector<uint64_t> &Mapping();

    /**
     * @brief Gets a free file page. The implementation now returns the smaller
     * file page from the free file page pool. It's more desirable to return
     * from a file who has most free pages. This would reduce replication
     * amplification, when replication granularity is on files.
     *
     * @return The free file page Id, if the free page pool is not empty. Or,
     * UINT32_MAX.
     */
    uint32_t GetFreeFilePage();

    std::shared_ptr<MappingSnapshot> mapping_;
    uint32_t free_page_head_{UINT32_MAX};

    std::set<uint32_t> free_file_pages_;

    uint32_t min_file_page_id_{1};
    uint32_t max_file_page_id_{0};
};

}  // namespace kvstore