#pragma once

#include <memory>

#include "kv_options.h"
#include "task.h"

namespace kvstore
{
class AsyncIoManager;
class IndexPageManager;
class StorageManager;

inline KvOptions kv_options{};

inline thread_local KvTask *thd_task;
extern thread_local std::unique_ptr<IndexPageManager> idx_manager;
extern thread_local std::unique_ptr<StorageManager> storage_manager;
}  // namespace kvstore