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
class TieringPolicy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  TieringPolicy(size_t scale_factor, size_t buffer_size, size_t modifier=0)
      : m_scale_factor(scale_factor), m_buffer_size(buffer_size), m_size_modifier(modifier) {}

  std::vector<ReconstructionVector> get_reconstruction_tasks(
      const Version<ShardType, QueryType> *version, LockManager &lock_mngr) const override {
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

    for (level_index i = target_level; i > source_level; i--) {
      size_t total_reccnt = levels[i - 1]->get_record_count();

      std::vector<ShardID> shards;
      for (ssize_t j=0; j<(ssize_t)levels[i-1]->get_shard_count(); j++) {
        shards.push_back({i-1, j});
      }

      if (total_reccnt > 0 || shards.size() > 0) {
        reconstructions.add_reconstruction(shards, i, total_reccnt, ReconstructionType::Compact);
      }
    }

    return reconstructions;
  }

private:
  level_index find_reconstruction_target(LevelVector &levels, size_t reccnt) const {
    level_index target_level = invalid_level_idx;

    for (level_index i = 1; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_shard_count() + 1 <= capacity(reccnt)) {
        target_level = i;
        break;
      }
    }

    return target_level;
  }

  inline size_t capacity(size_t reccnt) const { return m_scale_factor * std::pow(std::log(reccnt), m_size_modifier); }

  size_t m_scale_factor;
  size_t m_buffer_size;
  size_t m_size_modifier;
};
} // namespace de
