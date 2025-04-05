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

    new_struct->m_deleted_shards = m_deleted_shards;

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


  /*
   * Perform the reconstruction described by task. If the resulting
   * reconstruction grows the structure (i.e., adds a level), returns
   * true. Otherwise, returns false.
   */
  inline reconstruction_results<ShardType> perform_reconstruction(ReconstructionTask task) const { 
    reconstruction_results<ShardType> result;
    result.target_level = task.target;
    
    std::vector<const ShardType *> shards;
    for (ShardID shid : task.sources) {
      assert(shid.level_idx < (level_index) m_levels.size());
      assert(shid.shard_idx >= -1);

      auto raw_shard_ptr = m_levels[shid.level_idx]->get_shard(shid.shard_idx);
      shards.push_back(raw_shard_ptr);
      result.source_shards.emplace_back(shid.level_idx, raw_shard_ptr);
    }

    result.new_shard = std::make_shared<ShardType>(shards);

    return result;
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

  bool append_shard(std::shared_ptr<ShardType> shard, size_t version, size_t level) {
    assert(level <= m_levels.size());
    auto rc = false;

    if (level == m_levels.size()) {
      /* grow the structure */
      m_levels.push_back(std::make_shared<InternalLevel<ShardType, QueryType>>(level));
      rc = true;
    }

    m_levels[level]->append(shard, version);

    return rc;
  }

  void delete_shards(std::vector<std::pair<level_index, const ShardType*>> shards) {
    for (size_t i=0; i<shards.size(); i++) {
      assert(shards[i].first < (level_index) m_levels.size());
      ssize_t shard_idx = -1;
      for (size_t j=0; j<m_levels[shards[i].first]->get_shard_count(); j++) {
        if (m_levels[shards[i].first]->get_shard_ptr(j).first.get() == shards[i].second) {
          shard_idx = j;
          break;
        }
      }

      if (shard_idx != -1) {
        m_levels[shards[i].first]->delete_shard(shard_idx);
      } else {
        fprintf(stderr, "ERROR: failed to delete shard %ld\t%p\n", shards[i].first, shards[i].second);
        //exit(EXIT_FAILURE);
      }
    }
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

    void print_structure(bool debug=false) const {
      for (size_t i=0; i<m_levels.size(); i++) {
        if (debug) {
          fprintf(stdout, "[D] [%ld]:\t", i);
        } else {
          fprintf(stdout, "[%ld]:\t",  i);
        }

        if (m_levels[i]) {
          for (size_t j=0; j<m_levels[i]->get_shard_count(); j++) {
            fprintf(stdout, "(%ld, %ld, %p: %ld) ", j, m_levels[i]->get_shard_ptr(j).second, m_levels[i]->get_shard_ptr(j).first.get(), m_levels[i]->get_shard(j)->get_record_count()); 
          }
        } else {
          fprintf(stdout, "[Empty]");
        }

        fprintf(stdout, "\n");
      }
    }    

private:
  LevelVector m_levels;
  std::vector<ShardType *> m_deleted_shards;
};

} // namespace de
