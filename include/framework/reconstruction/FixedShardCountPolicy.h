/*
 * include/framework/reconstruction/FixedShardCountPolicy.h
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
class FixedShardCountPolicy : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  FixedShardCountPolicy(size_t buffer_size, size_t shard_count, size_t max_record_count) 
  : m_buffer_size(buffer_size), m_shard_count(shard_count), m_max_reccnt(max_record_count) {}

  ReconstructionVector
  get_reconstruction_tasks(const Version<ShardType, QueryType> *version,
                           size_t incoming_reccnt) const override {
    ReconstructionVector reconstructions;
    return reconstructions;

  }

  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {

    auto levels = version->get_structure()->get_level_vector();

    ReconstructionVector v;

    if (levels.size() == 0) {
      v.add_reconstruction(ReconstructionTask{
          {{buffer_shid}}, 0, m_buffer_size, ReconstructionType::Append});
      return v;
    }

    ShardID last_shid = {0, (shard_index) (levels[0]->get_shard_count() - 1)};

    if (levels[0]->get_shard(last_shid.shard_idx)->get_record_count() + m_buffer_size <= capacity()) {
        v.add_reconstruction(ReconstructionTask{
          {{buffer_shid, last_shid}}, 0, m_buffer_size, ReconstructionType::Merge});
        return v;
    } else {
        v.add_reconstruction(ReconstructionTask{
          {{buffer_shid}}, 0, m_buffer_size, ReconstructionType::Append});
        return v;
    }
  }

private:
  inline size_t capacity() const {
    double bps = (double) m_max_reccnt / (double) m_buffer_size / m_shard_count;
    return ceil(bps) * m_buffer_size;
  }

  size_t m_buffer_size;
  size_t m_shard_count;
  size_t m_max_reccnt;
};
} // namespace de
