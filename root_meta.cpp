#include "root_meta.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>

#include "coding.h"
#include "page_mapper.h"

namespace eloqstore
{

ManifestBuilder::ManifestBuilder()
{
    buff_.resize(header_bytes);
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::InvalidValue);
}

std::string_view ManifestBuilder::Snapshot(
    PageId root_id,
    PageId ttl_root,
    const MappingSnapshot *mapping,
    FilePageId max_fp_id,
    std::string_view dict_bytes,
    const FilePageIdTermMapping *term_mapping)
{
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1));
    PutVarint64(&buff_, max_fp_id);
    term_mapping_buf_.clear();
    if (term_mapping != nullptr)
    {
        term_mapping->Serialize(term_mapping_buf_);
    }

    uint32_t dict_len = dict_bytes.size();
    PutVarint32(&buff_, dict_len);
    buff_.append(dict_bytes.data(), dict_bytes.size());

    PutVarint32(&buff_, term_mapping_buf_.size());
    buff_.append(term_mapping_buf_.data(), term_mapping_buf_.size());
    mapping->Serialize(buff_);
    return Finalize(root_id, ttl_root);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

uint32_t ManifestBuilder::CurrentSize() const
{
    return buff_.size();
}

std::string_view ManifestBuilder::Finalize(PageId new_root, PageId ttl_root)
{
    EncodeFixed32(buff_.data() + offset_root, new_root);
    EncodeFixed32(buff_.data() + offset_ttl_root, ttl_root);

    uint32_t len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, len);

    SetChecksum(buff_);
    return buff_;
}

std::string_view ManifestBuilder::BuffView() const
{
    return buff_;
}

void RootMeta::Pin()
{
    ref_cnt_++;
}

void RootMeta::Unpin()
{
    assert(ref_cnt_ > 0);
    ref_cnt_--;
}

void CowRootMeta::UpdateTerm(Term term) const
{
    page_id_term_mapping_->Append(mapper_->FilePgAllocator()->MaxFilePageId(),
                                  term);
}

Term RootMeta::CurrentTerm() const
{
    return page_id_term_mapping_->CurrentTerm();
}

void FilePageIdTermMapping::Append(FilePageId file_page_id, Term term)
{
    mapping_.emplace_back(file_page_id, term);
}

Term FilePageIdTermMapping::CurrentTerm()
{
    return mapping_.empty() ? 0 : mapping_.back().second;
}

Term FilePageIdTermMapping::TermOf(FilePageId file_page_id) const
{
    if (mapping_.empty())
    {
        return 0;
    }
    auto it = std::upper_bound(
        mapping_.begin(),
        mapping_.end(),
        file_page_id,
        [](FilePageId value, const std::pair<FilePageId, Term> &entry)
        { return value < entry.first; });
    assert(it != mapping_.begin());
    if (it == mapping_.end())
    {
        return mapping_.back().second;
    }
    return std::prev(it)->second;
}

void FilePageIdTermMapping::Serialize(std::string &dst) const
{
    PutVarint64(&dst, mapping_.size());
    for (const auto &[file_page_id, term] : mapping_)
    {
        PutVarint64(&dst, file_page_id);
        PutVarint64(&dst, term);
    }
}

void FilePageIdTermMapping::Deserialize(std::string_view &src)
{
    mapping_.clear();
    size_t mapping_cnt;
    [[maybe_unused]] bool ok = GetVarint64(&src, &mapping_cnt);
    assert(ok);
    while (!src.empty())
    {
        FilePageId file_page_id;
        ok = GetVarint64(&src, &file_page_id);
        assert(ok);
        Term term;
        ok = GetVarint64(&src, &term);
        assert(ok);
        mapping_.emplace_back(file_page_id, term);
    }
}

}  // namespace eloqstore
