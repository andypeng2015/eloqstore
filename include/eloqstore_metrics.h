/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#ifdef ELOQSTORE_WITH_TXSERVICE
#include <cstddef>
#include <memory>
#include <string>

#include "meter.h"
#include "metrics.h"

namespace metrics
{
inline const Name NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION{
    "eloqstore_work_one_round_duration"};
inline const Name NAME_ELOQSTORE_ASYNC_IO_SUBMIT_DURATION{
    "eloqstore_async_io_submit_duration"};
inline const Name NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS{
    "eloqstore_task_manager_active_tasks"};

/**
 * @brief Register eloqstore metrics for a single shard.
 * 
 * Creates a meter with shard_id already in common labels, so no need to
 * pass shard_id when collecting metrics.
 * 
 * @param meter Output parameter: will be set to the created meter
 * @param metrics_registry Pointer to metrics registry
 * @param common_labels Common labels (node_ip, node_port, etc.)
 * @param shard_id Shard ID to add to common labels
 */
inline void register_eloqstore_metrics_for_shard(
    std::unique_ptr<Meter> &meter,
    MetricsRegistry *metrics_registry,
    const CommonLabels &common_labels,
    size_t shard_id)
{
    if (metrics_registry == nullptr)
    {
        return;
    }

    // Create common labels with shard_id included
    CommonLabels shard_common_labels = common_labels;
    shard_common_labels["shard_id"] = std::to_string(shard_id);

    // Create meter with shard_id in common labels
    meter = std::make_unique<Meter>(metrics_registry, shard_common_labels);

    // Register all three metrics without shard_id label (already in common labels)
    meter->Register(NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION, Type::Histogram);
    meter->Register(NAME_ELOQSTORE_ASYNC_IO_SUBMIT_DURATION, Type::Histogram);
    meter->Register(NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS, Type::Gauge);
}
}  // namespace metrics
#endif  // ELOQSTORE_WITH_TXSERVICE

