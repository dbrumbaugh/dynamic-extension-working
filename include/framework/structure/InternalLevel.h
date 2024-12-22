/*
 * include/framework/structure/InternalLevel.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * The word `Internal` in this class's name refers to memory. The current
 * model, inherited from the framework in Practical Dynamic Extension for
 * Sampling Indexes, would use a different ExternalLevel for shards stored
 * on external storage. This is a distinction that can probably be avoided
 * with some more thought being put into interface design.
 *
 */
#pragma once

#include <memory>
#include <vector>

#include "framework/interface/Query.h"
#include "framework/interface/Shard.h"
#include "util/types.h"

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class InternalLevel {
  typedef typename ShardType::RECORD RecordType;
  typedef BufferView<RecordType> BuffView;

public:
  InternalLevel(ssize_t level_no) : m_level_no(level_no) {}

  ~InternalLevel() = default;

  /*
   * Create a new shard containing the combined records
   * from all shards on this level and return it.
   *
   * No changes are made to this level.
   */
  ShardType *get_combined_shard() {
    if (m_shards.size() == 0) {
      return nullptr;
    }

    std::vector<ShardType *> shards;
    for (auto shard : m_shards) {
      if (shard)
        shards.emplace_back(shard.get());
    }

    return new ShardType(shards);
  }

  void get_local_queries(
      std::vector<std::pair<ShardID, ShardType *>> &shards,
      std::vector<typename QueryType::LocalQuery *> &local_queries,
      typename QueryType::Parameters *query_parms) {
    for (size_t i = 0; i < m_shards.size(); i++) {
      if (m_shards[i]) {
        auto local_query =
            QueryType::local_preproc(m_shards[i].get(), query_parms);
        shards.push_back({{m_level_no, (ssize_t)i}, m_shards[i].get()});
        local_queries.emplace_back(local_query);
      }
    }
  }

  bool check_tombstone(size_t shard_stop, const RecordType &rec) {
    if (m_shards.size() == 0)
      return false;

    for (int i = m_shards.size() - 1; i >= (ssize_t)shard_stop; i--) {
      if (m_shards[i]) {
        auto res = m_shards[i]->point_lookup(rec, true);
        if (res && res->is_tombstone()) {
          return true;
        }
      }
    }
    return false;
  }

  bool delete_record(const RecordType &rec) {
    if (m_shards.size() == 0)
      return false;

    for (size_t i = 0; i < m_shards.size(); ++i) {
      if (m_shards[i]) {
        auto res = m_shards[i]->point_lookup(rec);
        if (res) {
          res->set_delete();
          return true;
        }
      }
    }

    return false;
  }

  ShardType *get_shard(size_t idx) {
    if (idx >= m_shards.size()) {
      return nullptr;
    }

    return m_shards[idx].get();
  }

  size_t get_shard_count() { return m_shards.size(); }

  size_t get_record_count() {
    size_t cnt = 0;
    for (size_t i = 0; i < m_shards.size(); i++) {
      if (m_shards[i]) {
        cnt += m_shards[i]->get_record_count();
      }
    }

    return cnt;
  }

  size_t get_tombstone_count() {
    size_t res = 0;
    for (size_t i = 0; i < m_shards.size(); ++i) {
      if (m_shards[i]) {
        res += m_shards[i]->get_tombstone_count();
      }
    }
    return res;
  }

  size_t get_aux_memory_usage() {
    size_t cnt = 0;
    for (size_t i = 0; i < m_shards.size(); i++) {
      if (m_shards[i]) {
        cnt += m_shards[i]->get_aux_memory_usage();
      }
    }

    return cnt;
  }

  size_t get_memory_usage() {
    size_t cnt = 0;
    for (size_t i = 0; i < m_shards.size(); i++) {
      if (m_shards[i]) {
        cnt += m_shards[i]->get_memory_usage();
      }
    }

    return cnt;
  }

  double get_tombstone_prop() {
    size_t tscnt = 0;
    size_t reccnt = 0;
    for (size_t i = 0; i < m_shards.size(); i++) {
      if (m_shards[i]) {
        tscnt += m_shards[i]->get_tombstone_count();
        reccnt += m_shards[i]->get_record_count();
      }
    }

    return (double)tscnt / (double)(tscnt + reccnt);
  }

  std::shared_ptr<InternalLevel> clone() {
    auto new_level = std::make_shared<InternalLevel>(m_level_no);
    for (size_t i = 0; i < m_shards.size(); i++) {
      new_level->m_shards[i] = m_shards[i];
    }

    return new_level;
  }

  void truncate() { m_shards.erase(m_shards.begin(), m_shards.end()); }

  void delete_shard(shard_index shard) {
    m_shards.erase(m_shards.begin() + shard);
  }

  bool append(std::shared_ptr<ShardType> shard) {
    m_shards.emplace_back(shard);
  }

private:
  ssize_t m_level_no;
  std::vector<std::shared_ptr<ShardType>> m_shards;
};

} // namespace de
