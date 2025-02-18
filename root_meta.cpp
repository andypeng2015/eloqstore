#include "root_meta.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "coding.h"
#include "crc32.h"
#include "page_mapper.h"

namespace kvstore
{

ManifestBuilder::ManifestBuilder()
{
    buff_.resize(header_bytes);
}

void ManifestBuilder::UpdateMapping(uint32_t page_id, uint32_t file_page)
{
    PutVarint32(&buff_, page_id);
    PutVarint32(&buff_, file_page);
}

std::string_view ManifestBuilder::Snapshot(uint32_t root_id,
                                           const PageMapper &mapper)
{
    Reset();
    mapper.Serialize(buff_);
    return Finalize(root_id);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

std::string_view ManifestBuilder::Finalize(uint32_t new_root)
{
    uint32_t len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, len);

    EncodeFixed32(buff_.data() + offset_root, new_root);

    uint32_t crc =
        crc32::Value(buff_.data() + crc_bytes, buff_.size() - crc_bytes);
    EncodeFixed32(buff_.data(), crc32::Mask(crc));
    return buff_;
}

std::string_view ManifestBuilder::BuffView() const
{
    return buff_;
}

std::string ManifestBuilder::EmptySnapshot()
{
    std::string snap;
    snap.resize(header_bytes);
    EncodeFixed32(snap.data() + offset_len, 0);
    EncodeFixed32(snap.data() + offset_root, UINT32_MAX);
    uint32_t crc =
        crc32::Value(snap.data() + crc_bytes, header_bytes - crc_bytes);
    EncodeFixed32(snap.data(), crc32::Mask(crc));
    return snap;
}

bool RootMeta::Evict()
{
    if (ref_cnt_ == 1 && mapping_snapshots_.size() == 1 &&
        mapper_->UseCount() == 1)
    {
        assert(root_page_ != nullptr);
        assert(*mapping_snapshots_.begin() == mapper_->GetMapping());
        mapper_->FreeMappingSnapshot();
        assert(mapping_snapshots_.empty());
        mapper_ = nullptr;
        root_page_ = nullptr;
        ref_cnt_--;
        return true;
    }
    return false;
}

}  // namespace kvstore