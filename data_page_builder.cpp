#include "data_page_builder.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "coding.h"
#include "data_page.h"
#include "page_type.h"

namespace kvstore
{
DataPageBuilder::DataPageBuilder(const KvOptions *opt)
    : options_(opt), counter_(0), cnt_(0), finished_(false)
{
    buffer_.resize(HeaderSize(), 0x0);
    // The first restart point is at the offset 0.
    restarts_.emplace_back(buffer_.size());
}

void DataPageBuilder::Reset()
{
    buffer_.clear();
    buffer_.resize(HeaderSize(), 0x0);
    buffer_[0] = static_cast<char>(PageType::Data);
    restarts_.clear();
    restarts_.emplace_back(buffer_.size());
    counter_ = 0;
    cnt_ = 0;
    finished_ = false;
    last_key_.clear();
    last_timestamp_ = 0;
}

size_t DataPageBuilder::CurrentSizeEstimate() const
{
    return (buffer_.size() +                       // Raw data buffer
            restarts_.size() * sizeof(uint32_t) +  // Restart array
            sizeof(uint32_t));                     // Restart array length
}

size_t DataPageBuilder::HeaderSize()
{
    return 1 +                    // 1 byte for the page type.
           sizeof(uint16_t) +     // 2 bytes for content size
           sizeof(uint32_t) * 2;  // 2 * 4 bytes for IDs of prev and next pages
}

std::string_view DataPageBuilder::Finish()
{
    // Append restart array
    for (size_t i = 0; i < restarts_.size(); i++)
    {
        PutFixed16(&buffer_, restarts_[i]);
    }
    PutFixed16(&buffer_, restarts_.size());

    uint16_t content_size = buffer_.size();
    // Stores the page size at the header after the page type.
    EncodeFixed16(buffer_.data() + DataPage::page_size_offset, content_size);

    assert(buffer_.size() <= options_->data_page_size);
    buffer_.resize(options_->data_page_size);

    finished_ = true;
    return {buffer_.data(), buffer_.size()};
}

bool DataPageBuilder::Add(std::string_view key,
                          std::string_view value,
                          uint64_t ts)
{
#ifndef NDEBUG
    size_t buf_prev_size = buffer_.size();
    bool is_restart = false;
#endif

    std::string_view last_key_view{last_key_.data(), last_key_.size()};
    assert(!finished_);
    assert(counter_ <= options_->data_page_restart_interval);
    assert(buffer_.empty() ||
           options_->comparator_->Compare(key, last_key_view) > 0);
    // The number of bytes incurred by adding the key-value pair.
    size_t addition_delta = 0;
    size_t shared = 0;
    if (counter_ < options_->data_page_restart_interval)
    {
        // See how much sharing to do with previous string
        const size_t min_length = std::min(last_key_view.size(), key.size());
        while ((shared < min_length) && (last_key_view[shared] == key[shared]))
        {
            shared++;
        }
    }
    else
    {
        // Restarting compression adds one more restart point
        addition_delta += sizeof(uint16_t);
        last_timestamp_ = 0;
#ifndef NDEBUG
        is_restart = true;
#endif
    }
    size_t non_shared = key.size() - shared;

    // The data entry starts with the triple <shared><non_shared><value_size>
    addition_delta += Varint32Size(shared);
    addition_delta += Varint32Size(non_shared);
    addition_delta += Varint32Size(value.size());
    // Key
    addition_delta += non_shared;
    // Value
    addition_delta += value.size();

    // Timestamp delta
    assert(ts <= INT64_MAX);
    int64_t ts_delta = (int64_t) ts - last_timestamp_;
    uint64_t p_ts_delta = EncodeInt64Delta(ts_delta);
    addition_delta += Varint64Size(p_ts_delta);

    // Does not add the data item if it would overflow the page.
    if (CurrentSizeEstimate() + addition_delta > options_->data_page_size)
    {
        return false;
    }

    if (counter_ >= options_->data_page_restart_interval)
    {
        restarts_.emplace_back(buffer_.size());
        counter_ = 0;
    }

    // Adds the triple <shared><non_shared><value_size>
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value.size());

    // Adds string delta to buffer_ followed by value
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());
    PutVarint64(&buffer_, p_ts_delta);

#ifndef NDEBUG
    uint16_t minus = is_restart ? sizeof(uint16_t) : 0;
    assert(buffer_.size() - buf_prev_size == addition_delta - minus);
#endif

    // Update state
    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    last_timestamp_ = ts;
    assert(std::string_view(last_key_.data(), last_key_.size()) == key);
    ++counter_;
    ++cnt_;

    return true;
}
}  // namespace kvstore