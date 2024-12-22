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
class BSMPolicy : ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  BSMPolicy(size_t scale_factor, size_t buffer_size)
      : m_scale_factor(scale_factor), m_buffer_size(buffer_size) {}

  ReconstructionVector
  get_reconstruction_tasks(Epoch<ShardType, QueryType> *epoch,
                           size_t incoming_reccnt) override {
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
      if (i < levels.size()) {
        task.add_shard({i, all_shards_idx}, levels[i]->get_record_count());
      }
    }

    reconstructions.add_reconstruction(task);
    return reconstructions;
  }

  ReconstructionTask
  get_flush_task(Epoch<ShardType, QueryType> *epoch) override {
    return ReconstructionTask{
        {{buffer_shid}}, 0, m_buffer_size, ReconstructionType::Flush};
  }

private:
  level_index find_reconstruction_target(LevelVector &levels) {
    level_index target_level = 0;

    for (size_t i = 0; i < (level_index)levels.size(); i++) {
      if (levels[i].get_record_count() + 1 <= capacity(i)) {
        target_level = i;
        break;
      }
    }

    return target_level;
  }

  inline size_t capacity(level_index level) {
    return m_buffer_size * pow(m_scale_factor, level + 1);
  }

  size_t m_scale_factor;
  size_t m_buffer_size;
};
} // namespace de
