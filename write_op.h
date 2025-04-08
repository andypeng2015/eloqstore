#pragma once

#include <cstdint>

namespace kvstore
{
enum class WriteOp : uint8_t
{
    Upsert = 0,
    Delete
};
}