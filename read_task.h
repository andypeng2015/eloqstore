#pragma once

#include <butil/time.h>
#include <bvar/latency_recorder.h>

#include <string_view>

#include "error.h"
#include "task.h"
#include "types.h"

namespace eloqstore
{
class IndexPageManager;
class MemIndexPage;
class MappingSnapshot;

#ifdef ELOQ_MODULE_ENABLED
inline bvar::LatencyRecorder read_round("debug_1_read_round", "ns");
#endif

class ReadTask : public KvTask
{
public:
    KvError Read(const TableIdent &tbl_ident,
                 std::string_view search_key,
                 std::string &value,
                 uint64_t &timestamp,
                 uint64_t &expire_ts);

    /**
     * @brief Read the biggest key not greater than the search key.
     */
    KvError Floor(const TableIdent &tbl_id,
                  std::string_view search_key,
                  std::string &floor_key,
                  std::string &value,
                  uint64_t &timestamp,
                  uint64_t &expire_ts);

    TaskType Type() const override
    {
        return TaskType::Read;
    }

    void Record() override
    {
#ifdef ELOQ_MODULE_ENABLED
        if (need_record)
        {
            int64_t gap = butil::cpuwide_time_ns() - last_yield_ts;
            if (gap > 5000)
            {
                read_round << gap;
            }
        }
#endif
    }
};
}  // namespace eloqstore
