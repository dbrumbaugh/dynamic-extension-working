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
  BSMPolicy(size_t buffer_size, size_t scale_factor, size_t modifier = 0)
      : m_scale_factor(scale_factor), m_buffer_size(buffer_size),
        m_size_modifier(modifier) {}

  std::vector<ReconstructionVector>
  get_reconstruction_tasks(const Version<ShardType, QueryType> *version,
                           LockManager &lock_mngr) const override {
    return {};
  }

  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {
    ReconstructionVector reconstructions;
    auto levels = version->get_structure()->get_level_vector();

    level_index target_level = find_reconstruction_target(levels, version->get_structure()->get_record_count());
    assert(target_level != -1);
    level_index source_level = 0;

    if (target_level == invalid_level_idx) {
      /* grow */
      target_level = levels.size();
    }

    ReconstructionTask task;
    task.target = target_level;

    size_t reccnt = 0;
    if (target_level < (ssize_t)levels.size() && levels[target_level]->get_record_count() > 0) {
      task.sources.push_back({target_level, all_shards_idx});
      task.type = ReconstructionType::Merge;
    } else {
      task.type = ReconstructionType::Append;
    }

    for (level_index i = target_level - 1; i >= source_level; i--) {
      assert(i < (ssize_t)levels.size());
      assert(levels[i]->get_record_count() > 0 || i == 0);
      task.sources.push_back({i, all_shards_idx});
      reccnt += levels[i]->get_record_count();
    }

    task.reccnt = reccnt;
    assert(task.sources.size() > 0);
    reconstructions.add_reconstruction(task);

    return reconstructions;
  }

private:
  level_index find_reconstruction_target(LevelVector &levels, size_t reccnt) const {
    level_index target_level = invalid_level_idx;

    for (level_index i = 1; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() < capacity(i, reccnt)) {
        target_level = i;
        break;
      }
    }

    return target_level;
  }

  inline size_t capacity(level_index level, size_t reccnt) const {
    size_t base = m_scale_factor * pow(log(reccnt), m_size_modifier);
    return m_buffer_size * (base - 1) * pow(base, level + 1);
  }

  size_t m_scale_factor;
  size_t m_buffer_size;
  size_t m_size_modifier;
};
} // namespace de
