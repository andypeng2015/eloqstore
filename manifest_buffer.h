#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>

#include "coding.h"
#include "page.h"

namespace eloqstore
{

class ManifestBuffer
{
public:
    ManifestBuffer() = default;
    ManifestBuffer(ManifestBuffer &&other) noexcept
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_)
    {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }
    ManifestBuffer &operator=(ManifestBuffer &&other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }
        std::free(data_);
        data_ = other.data_;
        size_ = other.size_;
        capacity_ = other.capacity_;
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
        return *this;
    }
    ManifestBuffer(const ManifestBuffer &) = delete;
    ManifestBuffer &operator=(const ManifestBuffer &) = delete;
    ~ManifestBuffer()
    {
        std::free(data_);
    }

    void Reserve(size_t bytes)
    {
        EnsureCapacity(bytes);
    }

    void Resize(size_t new_size)
    {
        EnsureCapacity(new_size);
        size_ = new_size;
        ZeroTail();
    }

    void Reset()
    {
        size_ = 0;
        ZeroTail();
    }

    void Append(const char *src, size_t len)
    {
        if (len == 0)
        {
            return;
        }
        EnsureCapacity(size_ + len);
        std::memcpy(data_ + size_, src, len);
        size_ += len;
        ZeroTail();
    }

    void AppendVarint32(uint32_t value)
    {
        char buf[5];
        char *end = EncodeVarint32(buf, value);
        Append(buf, static_cast<size_t>(end - buf));
    }

    void AppendVarint64(uint64_t value)
    {
        char buf[10];
        char *end = EncodeVarint64(buf, value);
        Append(buf, static_cast<size_t>(end - buf));
    }

    char *Data()
    {
        return data_;
    }

    const char *Data() const
    {
        return data_;
    }

    size_t Size() const
    {
        return size_;
    }

    std::string_view View() const
    {
        return {data_, size_};
    }

    size_t PaddedSize() const
    {
        if (data_ == nullptr || size_ == 0)
        {
            return 0;
        }
        return AlignSize(size_);
    }

private:
    static size_t AlignSize(size_t size)
    {
        const size_t alignment = page_align;
        if (size == 0)
        {
            return alignment;
        }
        const size_t remainder = size % alignment;
        if (remainder == 0)
        {
            return size;
        }
        return size + (alignment - remainder);
    }

    void EnsureCapacity(size_t min_capacity)
    {
        if (min_capacity <= capacity_)
        {
            return;
        }
        const size_t alignment = page_align;
        size_t grow = capacity_ == 0 ? alignment : capacity_ * 2;
        size_t new_capacity =
            std::max(AlignSize(min_capacity), AlignSize(grow));
        char *new_data =
            static_cast<char *>(std::aligned_alloc(alignment, new_capacity));
        if (new_data == nullptr)
        {
            throw std::bad_alloc();
        }
        std::memset(new_data, 0, new_capacity);
        if (data_ != nullptr)
        {
            std::memcpy(new_data, data_, size_);
            std::free(data_);
        }
        data_ = new_data;
        capacity_ = new_capacity;
        ZeroTail();
    }

    void ZeroTail()
    {
        if (data_ == nullptr)
        {
            return;
        }
        if (data_ == nullptr)
        {
            return;
        }
        const size_t aligned = size_ == 0 ? 0 : AlignSize(size_);
        if (aligned > capacity_)
        {
            return;
        }
        const size_t padding = aligned - size_;
        if (padding > 0)
        {
            std::memset(data_ + size_, 0, padding);
        }
    }

    char *data_{nullptr};
    size_t size_{0};
    size_t capacity_{0};
};

}  // namespace eloqstore
