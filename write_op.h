#pragma once

#include <cstdint>

namespace kvstore
{
enum class WriteOp : uint8_t
{
    Insert = 0,
    Update,
    Delete,
    Upsert
};
}