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
#include "framework/scheduling/Version.h"
#include "util/types.h"

namespace de {
template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class BSMPolicy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  BSMPolicy(size_t buffer_size)
      : m_scale_factor(2), m_buffer_size(buffer_size) {}

  std::vector<ReconstructionVector>
  get_reconstruction_tasks(const Version<ShardType, QueryType> *version, LockManager &lock_mngr) const override {
    return {};
  }

  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {
    ReconstructionVector reconstructions;
    auto levels = version->get_structure()->get_level_vector();

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

    std::vector<ShardID> source_shards;
    size_t reccnt = 0;

    source_shards.push_back({0, all_shards_idx});

    for (level_index i = target_level; i > source_level; i--) {
      if (i < (level_index)levels.size()) {
        source_shards.push_back({i-1, all_shards_idx});
        reccnt += levels[i-1]->get_record_count();
      }
    }

    assert(source_shards.size() > 0);

    reconstructions.add_reconstruction(source_shards, target_level, reccnt, ReconstructionType::Merge);
    return reconstructions;
  }

private:
  level_index find_reconstruction_target(LevelVector &levels) const {
    level_index target_level = invalid_level_idx;

    for (level_index i = 1; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() == 0) {
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
