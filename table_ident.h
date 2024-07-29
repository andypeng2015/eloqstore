#pragma once

#include <cstdint>
#include <string>

namespace kvstore
{
struct TableIdent
{
    friend bool operator==(const TableIdent &lhs, const TableIdent &rhs)
    {
        return lhs.tbl_name_ == rhs.tbl_name_ &&
               lhs.partition_id_ == rhs.partition_id_;
    }

    std::string tbl_name_;
    uint32_t partition_id_;
};
}  // namespace kvstore

template <>
struct std::hash<kvstore::TableIdent>
{
    std::size_t operator()(const kvstore::TableIdent &tbl_ident) const
    {
        return std::hash<std::string>()(tbl_ident.tbl_name_) * 23 +
               std::hash<uint32_t>()(tbl_ident.partition_id_);
    }
};