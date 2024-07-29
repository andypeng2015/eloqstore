#pragma once

#include <cstdint>

namespace kvstore
{
enum struct PageType : uint8_t
{
    NonLeafIndex = 0,
    LeafIndex,
    Data,
    Deleted = 255
};
}