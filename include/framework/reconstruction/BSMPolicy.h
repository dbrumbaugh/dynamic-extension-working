/*
 * include/framework/reconstruction/LevelingPolicy.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once
#include <cmath>

#include "framework/reconstruction/ReconstructionPolicy.h"
#include "framework/scheduling/Epoch.h"
#include "util/types.h"

namespace de {
template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class BSMPolicy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  BSMPolicy(size_t buffer_size)
      : m_scale_factor(2), m_buffer_size(buffer_size) {}

  ReconstructionVector
  get_reconstruction_tasks(const Epoch<ShardType, QueryType> *epoch,
                           size_t incoming_reccnt) const override {
    ReconstructionVector reconstructions;
    auto levels = epoch->get_structure()->get_level_vector();

    level_index target_level = find_reconstruction_target(levels);
    assert(target_level != -1);
    level_index source_level = 0;

    if (target_level == invalid_level_idx) {
      /* grow */
      target_level = levels.size();
    }

    ReconstructionTask task;
    task.target = target_level;
    task.type = ReconstructionType::Merge;

    for (level_index i = target_level; i > source_level; i--) {
      if (i < (level_index)levels.size()) {
        task.add_shard({i, all_shards_idx}, levels[i]->get_record_count());
      }
    }

    reconstructions.add_reconstruction(task);
    return reconstructions;
  }

  ReconstructionTask
  get_flush_task(const Epoch<ShardType, QueryType> *epoch) const override {
    return ReconstructionTask{
        {{buffer_shid}}, 0, m_buffer_size, ReconstructionType::Merge};
  }

private:
  level_index find_reconstruction_target(LevelVector &levels) const {
    level_index target_level = invalid_level_idx;

    for (level_index i = 0; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() + m_buffer_size <= capacity(i)) {
        target_level = i;
        break;
      }
    }

    return target_level;
  }

  inline size_t capacity(level_index level) const {
    return m_buffer_size * pow(m_scale_factor, level + 1);
  }

  size_t m_scale_factor;
  size_t m_buffer_size;
};
} // namespace de
