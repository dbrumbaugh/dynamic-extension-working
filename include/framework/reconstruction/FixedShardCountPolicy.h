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
#include "framework/scheduling/Version.h"
#include "util/types.h"

namespace de {
template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class FixedShardCountPolicy
    : public ReconstructionPolicy<ShardType, QueryType> {
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;

public:
  FixedShardCountPolicy(size_t buffer_size, size_t shard_count,
                        size_t max_record_count)
      : m_buffer_size(buffer_size), m_shard_count(shard_count),
        m_max_reccnt(max_record_count) {}

  std::vector<ReconstructionVector>
  get_reconstruction_tasks(const Version<ShardType, QueryType> *version,
                           LockManager &lock_mngr) const override {
    return {};
  }

  ReconstructionVector
  get_flush_tasks(const Version<ShardType, QueryType> *version) const override {

    auto levels = version->get_structure()->get_level_vector();
    ReconstructionVector v;

    /* if this is the very first flush, there won't be an L1 yet */
    if (levels.size() > 1 && levels[1]->get_shard_count() > 0) {
      ShardID last_shid = {1, (shard_index)(levels[1]->get_shard_count() - 1)};
      if (levels[1]->get_shard(last_shid.shard_idx)->get_record_count() +
              m_buffer_size <=
          capacity()) {
        auto task = ReconstructionTask{
            {{0, 0}, last_shid}, 1, m_buffer_size, ReconstructionType::Merge};
        v.add_reconstruction(task);
        return v;
      }
    }

    auto task = ReconstructionTask{
        {{0, 0}}, 1, m_buffer_size, ReconstructionType::Append};
    v.add_reconstruction(task);
    return v;
  }

private:
  inline size_t capacity() const {
    double bps = (double)m_max_reccnt / (double)m_buffer_size / m_shard_count;
    return ceil(bps) * m_buffer_size;
  }

  size_t m_buffer_size;
  size_t m_shard_count;
  size_t m_max_reccnt;
};
} // namespace de
