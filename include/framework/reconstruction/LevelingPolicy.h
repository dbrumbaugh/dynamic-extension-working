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
  LevelingPolicy(size_t scale_factor, size_t buffer_size, double modifier = 0)
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

    level_index target_level = find_reconstruction_target(
        levels, version->get_structure()->get_record_count());
    assert(target_level != -1);
    level_index source_level = 0;

    if (target_level == invalid_level_idx) {
      /* grow */
      target_level = levels.size();
    }

    /*
     * For leveling, the only "actual" reconstruction happens at the target
     * level. All the other reconstructions simply shift the levels down
     * without needing to do any rebuilding.
     */
    size_t target_reccnt = (target_level < (level_index)levels.size())
                               ? levels[target_level]->get_record_count()
                               : 0;
    size_t total_reccnt =
        (target_level == 1)
            ? m_buffer_size + target_reccnt
            : levels[target_level - 1]->get_record_count() + target_reccnt;
    auto type = (target_level >= (level_index)levels.size())
                    ? ReconstructionType::Append
                    : ReconstructionType::Merge;
    reconstructions.add_reconstruction(target_level - 1, target_level,
                                       total_reccnt, type);

    /*
     * For all other levels, we'll shift them down by using a single-source
     * append.
     */
    for (level_index i = target_level - 1; i > source_level; i--) {
      reconstructions.add_reconstruction(i - 1, i, 0,
                                         ReconstructionType::Append);
    }
    return reconstructions;
  }

private:
  level_index find_reconstruction_target(LevelVector &levels,
                                         size_t reccnt) const {
    level_index target_level = invalid_level_idx;
    size_t incoming_records = m_buffer_size;

    for (level_index i = 1; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() + incoming_records <
          capacity(i, reccnt)) {
        target_level = i;
        break;
      }

      incoming_records = levels[i]->get_record_count();
    }

    return target_level;
  }

  inline size_t capacity(level_index level, size_t reccnt) const {
    return m_buffer_size *
           pow(m_scale_factor * std::ceil(std::pow<double>(std::log10(reccnt),
                                                           m_size_modifier)),
               level);
  }

  size_t m_scale_factor;
  size_t m_buffer_size;
  double m_size_modifier;
};
} // namespace de
