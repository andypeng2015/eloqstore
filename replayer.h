#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "async_io_manager.h"
#include "error.h"
#include "page_mapper.h"

namespace kvstore
{
class Replayer
{
public:
    Replayer();
    KvError Replay(ManifestFilePtr log, const KvOptions *opts);
    std::unique_ptr<PageMapper> Mapper(IndexPageManager *idx_mgr,
                                       const TableIdent *tbl_ident);

    uint32_t root_;
    std::unique_ptr<PageMapper> mapper_{nullptr};
    uint64_t file_size_;

private:
    KvError NextRecord(ManifestFile *log);
    KvError ReplayMapping(std::string_view);

    std::string log_buf_;
    std::string_view mapping_;
};
}  // namespace kvstore