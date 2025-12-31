#pragma once

#include "direct_io_buffer.h"
#include "pool.h"

namespace eloqstore
{
using DirectIoBufferPool = Pool<DirectIoBuffer>;
}  // namespace eloqstore
