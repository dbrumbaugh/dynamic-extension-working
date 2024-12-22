/*
 * include/framework/structure/ExtensionStructure.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <cstdio>
#include <memory>
#include <vector>

#include "framework/structure/BufferView.h"
#include "framework/structure/InternalLevel.h"
#include "util/types.h"

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class ExtensionStructure {
  typedef typename ShardType::RECORD RecordType;
  typedef BufferView<RecordType> BuffView;
  typedef std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>>
      LevelVector;
public:
  ExtensionStructure() = default;
  ~ExtensionStructure() = default;

  /*
   * Create a shallow copy of this extension structure. The copy will share
   * references to the same levels/shards as the original, but will have its
   * own lists. As all of the shards are immutable (with the exception of
   * deletes), the copy can be restructured with reconstructions and flushes
   * without affecting the original. The copied structure will be returned
   * with a reference count of 0; generally you will want to immediately call
   * take_reference() on it.
   *
   * NOTE: When using tagged deletes, a delete of a record in the original
   * structure will affect the copy, so long as the copy retains a reference
   * to the same shard as the original. This could cause synchronization
   * problems under tagging with concurrency. Any deletes in this context will
   * need to be forwarded to the appropriate structures manually.
   */
  ExtensionStructure<ShardType, QueryType> *copy() const {
    auto new_struct = new ExtensionStructure<ShardType, QueryType>();
    for (size_t i = 0; i < m_levels.size(); i++) {
      new_struct->m_levels.push_back(m_levels[i]->clone());
    }

    new_struct->m_refcnt = 0;
    return new_struct;
  }

  /*
   * Search for a record matching the argument and mark it deleted by
   * setting the delete bit in its wrapped header. Returns 1 if a matching
   * record was found and deleted, and 0 if a matching record was not found.
   *
   * This function will stop after finding the first matching record. It is
   * assumed that no duplicate records exist. In the case of duplicates, this
   * function will still "work", but in the sense of "delete first match".
   */
  int tagged_delete(const RecordType &rec) {
    for (auto level : m_levels) {
      if (level && level->delete_record(rec)) {
        return 1;
      }
    }

    /*
     * If the record to be erased wasn't found, return 0. The
     * DynamicExtension itself will then search the active
     * Buffers.
     */
    return 0;
  }

  /*
   * Return the total number of records (including tombstones) within all
   * of the levels of the structure.
   */
  size_t get_record_count() const {
    size_t cnt = 0;

    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_record_count();
    }

    return cnt;
  }

  /*
   * Return the total number of tombstones contained within all of the
   * levels of the structure.
   */
  size_t get_tombstone_count() const {
    size_t cnt = 0;

    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_tombstone_count();
    }

    return cnt;
  }

  /*
   * Return the number of levels within the structure. Note that not
   * all of these levels are necessarily populated.
   */
  size_t get_height() const { return m_levels.size(); }

  /*
   * Return the amount of memory (in bytes) used by the shards within the
   * structure for storing the primary data structure and raw data.
   */
  size_t get_memory_usage() const {
    size_t cnt = 0;
    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_memory_usage();
    }

    return cnt;
  }

  /*
   * Return the amount of memory (in bytes) used by the shards within the
   * structure for storing auxiliary data structures. This total does not
   * include memory used for the main data structure, or raw data.
   */
  size_t get_aux_memory_usage() const {
    size_t cnt = 0;
    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i]) {
        cnt += m_levels[i]->get_aux_memory_usage();
      }
    }

    return cnt;
  }

  inline void perform_reconstruction(ReconstructionTask task) {
    /* perform the reconstruction itself */
    std::vector<ShardType *> shards;
    for (ShardID shid : task.sources) {
      assert(shid.level_idx < m_levels.size());
      assert(shid.shard_idx >= -1);

      /* if unspecified, push all shards into the vector */
      if (shid.shard_idx == all_shards_idx) {
        for (size_t i = 0; i < m_levels[shid.level_idx].get_shard_count();
             i++) {
          if (m_levels[shid.level_idx]->get_shard(i)) {
            shards.push_back(m_levels[shid.level_idx]->get_shard(i));
          }
        }
      } else {
        shards.push_back(m_levels[shid.level_idx]->get_shard(shid.shard_idx));
      }
    }

    auto new_shard = Shard(shards);

    /*
     * Remove all of the shards processed by the operation
     */
    for (ShardID shid : task.sources) {
      if (shid.shard_idx == all_shards_idx) {
        m_levels[shid.level_idx]->truncate();
      } else {
        m_levels[shid.level_idx]->delete_shard(shid.shard_idx);
      }
    }

    /*
     * Append the new shard to the target level
     */
    if (task.target < m_levels.size()) {
      m_levels[task.target]->append_shard(new_shard);
    } else {
      m_levels.push_back();
    }
  }

  inline void perform_flush(ReconstructionTask task, BuffView buffer) {
    /*
     * FIXME: this might be faster with a custom interface for merging
     * the buffer and a vector of shards, but that would also complicate
     * the shard interface a lot, so we'll leave it like this for now. It
     * does mean that policies that merge the buffer into L0 double-process
     * the buffer itself. Given that we're unlikely to actually use policies
     * like that, we'll leave this as low priority.
     */
    ShardType *buffer_shard = new ShardType(buffer);
    if (task.type == ReconstructionType::Append) {
      m_levels[0]->append(std::shared_ptr(buffer_shard));
    } else {
      std::vector<ShardType *> shards;
      for (size_t i = 0; i < m_levels[0].size(); i++) {
        if (m_levels[0]->get_shard(i)) {
          shards.push_back(m_levels[0]->get_shard(i));
        }

        shards.push_back(buffer_shard);
        ShardType *new_shard = new ShardType(shards);
        m_levels[0]->truncate();
        m_levels[0]->append(std::shared_ptr(new_shard));
      }
    }
  }

  bool take_reference() {
    m_refcnt.fetch_add(1);
    return true;
  }

  bool release_reference() {
    assert(m_refcnt.load() > 0);
    m_refcnt.fetch_add(-1);
    return true;
  }

  size_t get_reference_count() const { return m_refcnt.load(); }

  std::vector<typename QueryType::LocalQuery *>
  get_local_queries(std::vector<std::pair<ShardID, ShardType *>> &shards,
                    typename QueryType::Parameters *parms) const {

    std::vector<typename QueryType::LocalQuery *> queries;

    for (auto &level : m_levels) {
      level->get_local_queries(shards, queries, parms);
    }

    return queries;
  }

  LevelVector const &get_level_vector() const { return m_levels; }

private:
  std::atomic<size_t> m_refcnt;
  LevelVector m_levels;
};

} // namespace de
