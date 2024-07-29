#pragma once

#include <cstring>
#include <string>
#include <unordered_map>

#include "error.h"
#include "table_ident.h"

namespace kvstore
{
class MemStorage
{
public:
    KvError Write(const TableIdent &tbl_ident,
                  uint32_t file_page_id,
                  const char *ptr,
                  size_t size)
    {
        auto idx_it = store_.try_emplace(tbl_ident);
        std::unordered_map<size_t, std::string> &map = idx_it.first->second;
        auto pit = map.try_emplace(file_page_id, ptr, size);
        if (!pit.second)
        {
            std::string &blob = pit.first->second;
            blob.resize(size);
            memcpy(blob.data(), ptr, size);
        }
        return KvError::NoError;
    }

    KvError Read(const TableIdent &tbl_ident,
                 uint32_t file_page_id,
                 char *ptr,
                 size_t size)
    {
        auto idx_it = store_.find(tbl_ident);
        if (idx_it == store_.end())
        {
            return KvError::NotFound;
        }
        else
        {
            const std::unordered_map<size_t, std::string> &map = idx_it->second;
            auto pit = map.find(file_page_id);
            if (pit == map.end())
            {
                return KvError::NotFound;
            }
            else
            {
                std::memcpy(ptr, pit->second.data(), size);
                return KvError::NoError;
            }
        }
    }

private:
    std::unordered_map<TableIdent, std::unordered_map<size_t, std::string>>
        store_;
};
}  // namespace kvstore