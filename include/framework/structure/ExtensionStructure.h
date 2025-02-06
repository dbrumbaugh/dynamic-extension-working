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
  ExtensionStructure(bool default_level=true) {
    if (default_level)
      m_levels.emplace_back(std::make_shared<InternalLevel<ShardType, QueryType>>(0));
  }
  
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
    auto new_struct = new ExtensionStructure<ShardType, QueryType>(false);
    for (size_t i = 0; i < m_levels.size(); i++) {
      new_struct->m_levels.push_back(m_levels[i]->clone());
    }

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

  size_t get_shard_count() const {
    size_t cnt = 0;
    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i]) {
        cnt += m_levels[i]->get_nonempty_shard_count();
      }
    }

    return cnt;
  }

  inline void perform_reconstruction(ReconstructionTask task, size_t version=0) { 
    /* perform the reconstruction itself */
    std::vector<const ShardType *> shards;
    for (ShardID shid : task.sources) {
      assert(shid.level_idx <= (level_index) m_levels.size());
      assert(shid.shard_idx >= -1);

      if (shid.level_idx == (level_index) m_levels.size()) {
        continue;
      }

      /* if unspecified, push all shards into the vector */
      if (shid.shard_idx == all_shards_idx) {
        for (size_t i = 0; i < m_levels[shid.level_idx]->get_shard_count();
             i++) {
          if (m_levels[shid.level_idx]->get_shard(i)) {
            shards.push_back(m_levels[shid.level_idx]->get_shard(i));
          }
        }
      } else {
        shards.push_back(m_levels[shid.level_idx]->get_shard(shid.shard_idx));
      }
    }

    auto new_shard = new ShardType(shards);

    /*
     * Remove all of the shards processed by the operation
     */
    for (ShardID shid : task.sources) {
      if (shid.level_idx == (level_index) m_levels.size()) {
        continue;
      } else if (shid.shard_idx == all_shards_idx) {
        m_levels[shid.level_idx]->truncate();
      } else if (shid != buffer_shid) {
        m_levels[shid.level_idx]->delete_shard(shid.shard_idx);
      }
    }

    // fprintf(stderr, "Target: %ld\tLevels:%ld\n", task.target, m_levels.size());

    /*
     * Append the new shard to the target level
     */
    if (task.target < (level_index)m_levels.size()) {
      m_levels[task.target]->append(std::shared_ptr<ShardType>(new_shard), version);
      // fprintf(stderr, "append (no growth)\n");
    } else { /* grow the structure if needed */
      m_levels.push_back(std::make_shared<InternalLevel<ShardType, QueryType>>(task.target));
      m_levels[task.target]->append(std::shared_ptr<ShardType>(new_shard), version);
      // fprintf(stderr, "grow and append\n");
    }
  }

  std::vector<typename QueryType::LocalQuery *>
  get_local_queries(std::vector<std::pair<ShardID, ShardType *>> &shards,
                    typename QueryType::Parameters *parms) const {

    std::vector<typename QueryType::LocalQuery *> queries;

    for (auto &level : m_levels) {
      level->get_local_queries(shards, queries, parms);
    }

    return queries;
  }

  size_t l0_size() const {
    return m_levels[0]->get_shard_count();
  }

  void append_l0(std::shared_ptr<ShardType> shard, size_t version) {
    m_levels[0]->append(shard, version);
  }

  LevelVector const &get_level_vector() const { return m_levels; }

  
    /*
     * Validate that no level in the structure exceeds its maximum tombstone
     * capacity. This is used to trigger preemptive compactions at the end of
     * the reconstruction process.
     */
    bool validate_tombstone_proportion(double max_delete_prop) const {
      long double ts_prop;
      for (size_t i = 0; i < m_levels.size(); i++) {
        if (m_levels[i]) {
          ts_prop = (long double)m_levels[i]->get_tombstone_count() /
                    (long double)m_levels[i]->get_record_count();
          if (ts_prop > (long double)max_delete_prop) {
            return false;
          }
        }
      }

      return true;
    }

    bool validate_tombstone_proportion(level_index level, double max_delete_prop) const {
        long double ts_prop =  (long double) m_levels[level]->get_tombstone_count() / (long double) m_levels[level]->get_record_count();
        return ts_prop <= (long double) max_delete_prop;
    }

    void print_structure() const {
      for (size_t i=0; i<m_levels.size(); i++) {
        fprintf(stdout, "[%ld]:\t", i);

        if (m_levels[i]) {
          for (size_t j=0; j<m_levels[i]->get_shard_count(); j++) {
            fprintf(stdout, "(%ld: %ld) ", j, m_levels[i]->get_shard(j)->get_record_count()); 
          }
        } else {
          fprintf(stdout, "[Empty]");
        }

        fprintf(stdout, "\n");
      }
    }    


    void merge_structure(const ExtensionStructure* old_structure, size_t version_id = 0) {
      assert(version_id > 0);

      for (size_t i=0; i<old_structure->m_levels.size(); i++) {
        for (size_t j=0; j<old_structure->m_levels[i]->get_shard_count(); j++) {
          if (old_structure->m_levels[i]->get_shard_version(j) > version_id) {
            m_levels[i]->append(old_structure->m_levels[i]->get_shard_ptr(j));
          }
        }
      }
    }

    void update_shard_version(size_t version) {
      assert(version != 0);

      for (size_t i=0; i<m_levels.size(); i++) {
        for (size_t j=0; j<m_levels[i]->get_shard_count(); j++) {
          if (m_levels[i]->get_shard_version(j) == 0) {
            m_levels[i]->set_shard_version(j, version);
          }
        }
      }
    }

private:
  LevelVector m_levels;
};

} // namespace de
