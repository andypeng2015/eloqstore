#pragma once

#include <cstdint>
#include <string_view>

#include "coding.h"

inline std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = kvstore::ToBigEndian(key);
    kvstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

inline uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = kvstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}

void InitEnv();
void InitData(const std::string &tbl_name,
              uint32_t partition_id,
              size_t data_size,
              uint64_t data_ts);