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
class CompactOnFull : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  CompactOnFull(size_t scale_factor, size_t buffer_size, size_t size_modifier=0)
      : m_scale_factor(scale_factor), m_buffer_size(buffer_size), m_size_modifier(size_modifier) {}

  std::vector<ReconstructionVector> get_reconstruction_tasks(
      const Version<ShardType, QueryType> *version, LockManager &lock_mngr) const override {
    std::vector<ReconstructionVector> reconstructions;

    auto levels = version->get_structure()->get_level_vector();

    if (levels[0]->get_shard_count() == 0) {
      return {};
    }

    for (level_index i=0; i < (ssize_t) levels.size(); i++) {
      if (levels[i]->get_shard_count() >= m_scale_factor && lock_mngr.take_lock(i, version->get_id())) {
        ReconstructionVector recon;
        size_t total_reccnt = levels[i]->get_record_count();
        std::vector<ShardID> shards;
        for (ssize_t j=0; j<(ssize_t) levels[i]->get_shard_count(); j++) {
          shards.push_back({i,j});
        }

        recon.add_reconstruction(shards, i+1, total_reccnt, ReconstructionType::Compact);
        reconstructions.push_back(recon);
      }
    }

      return reconstructions;
    }


  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {
    ReconstructionVector reconstructions;

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
