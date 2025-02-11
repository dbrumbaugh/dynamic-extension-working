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
class LevelingPolicy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  LevelingPolicy(size_t scale_factor, size_t buffer_size)
      : m_scale_factor(scale_factor), m_buffer_size(buffer_size) {}

  std::vector<ReconstructionVector>
  get_reconstruction_tasks(const Version<ShardType, QueryType> *version, LockManager &lock_mngr) const override { 
    return {};
  }

  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {
    ReconstructionVector reconstructions;
    auto levels = version->get_structure()->get_level_vector();

    level_index target_level = find_reconstruction_target(levels);
    level_index source_level = 0;

    if (target_level == invalid_level_idx) {
      /* grow */
      target_level = levels.size();
    }

    for (level_index i = target_level; i > source_level; i--) {
      size_t target_reccnt =
          (i < (level_index)levels.size()) ? levels[i]->get_record_count() : 0;
      size_t total_reccnt =
          (i == 0) ? m_buffer_size + target_reccnt
                   : levels[i - 1]->get_record_count() + target_reccnt;

      reconstructions.add_reconstruction(i - 1, i, total_reccnt,
                                         ReconstructionType::Merge);
    }

    return reconstructions;
  }

private:
  level_index find_reconstruction_target(LevelVector &levels) const {
    level_index target_level = invalid_level_idx;
    size_t incoming_records = m_buffer_size;

    for (level_index i = 1; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() + incoming_records < capacity(i)) {
        target_level = i;
        break;
      }

      incoming_records = levels[i]->get_record_count();
    }

    return target_level;
  }

  inline size_t capacity(level_index level) const {
    return m_buffer_size * pow(m_scale_factor, level);
  }

  size_t m_scale_factor;
  size_t m_buffer_size;
};
} // namespace de
