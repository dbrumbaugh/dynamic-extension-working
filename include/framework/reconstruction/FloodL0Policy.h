/*
 * include/framework/reconstruction/FloodL0Policy.h
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
class FloodL0Policy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  FloodL0Policy(size_t buffer_size) : m_buffer_size(buffer_size) {}

  ReconstructionVector
  get_reconstruction_tasks(const Epoch<ShardType, QueryType> *epoch,
                           size_t incoming_reccnt) const override {

    ReconstructionVector reconstructions;
    return reconstructions;

  }

  ReconstructionTask
  get_flush_task(const Epoch<ShardType, QueryType> *epoch) const override {
    return ReconstructionTask{
        {{buffer_shid}}, 0, m_buffer_size, ReconstructionType::Append};
  }

private:
  level_index find_reconstruction_target(LevelVector &levels) const {
    level_index target_level = invalid_level_idx;
    size_t incoming_records = m_buffer_size;

    for (level_index i = 0; i < (level_index)levels.size(); i++) {
      if (levels[i]->get_record_count() + incoming_records < capacity(i)) {
        target_level = i;
        break;
      }

      incoming_records = levels[i]->get_record_count();
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
