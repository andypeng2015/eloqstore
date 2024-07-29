#include "common.h"

#include <memory>

#include "index_page_manager.h"
#include "storage_manager.h"
#include "write_task.h"

thread_local std::unique_ptr<kvstore::IndexPageManager> kvstore::idx_manager =
    nullptr;
thread_local std::unique_ptr<kvstore::StorageManager> kvstore::storage_manager =
    nullptr;

void InitEnv()
{
    if (kvstore::storage_manager == nullptr)
    {
        kvstore::storage_manager = std::make_unique<kvstore::StorageManager>();
        kvstore::idx_manager = std::make_unique<kvstore::IndexPageManager>(
            &kvstore::kv_options, kvstore::storage_manager->GetIoManager());
    }
    else
    {
        assert(kvstore::idx_manager != nullptr);
    }
}

void InitData(const std::string &tbl_name,
              uint32_t partition_id,
              size_t data_size,
              uint64_t data_ts)
{
    kvstore::WriteTask write_task{tbl_name, partition_id, &kvstore::kv_options};
    kvstore::thd_task = &write_task;
    write_task.Reset(kvstore::idx_manager.get());

    for (size_t idx = 0; idx < data_size; ++idx)
    {
        char buf[sizeof(uint64_t)];
        ConvertIntKey(buf, idx);
        std::string val = std::to_string(idx);
        write_task.AddData(std::string{buf, sizeof(uint64_t)},
                           std::move(val),
                           data_ts,
                           kvstore::WriteOp::Upsert);
    }

    write_task.Apply();
}