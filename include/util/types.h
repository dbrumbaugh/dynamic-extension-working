/*
 * include/util/types.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A centralized header file for various data types used throughout the
 * code base. There are a few very specific types, such as header formats,
 * that are defined within the header files that make direct use of them,
 * but all generally usable, simple types are defined here.
 *
 * Many of these types were used in the Practical Dynamic Extension for
 * Sampling Indexes work, particularly for external storage and buffer
 * pool systems. They aren't used now, but we're leaving them here to use
 * them in the future, when we add this functionality into this system too.
 */
#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <vector>

namespace de {

/* Represents a page offset within a specific file (physical or virtual) */
typedef uint32_t PageNum;

/*
 * Byte offset within a page. Also used for lengths of records, etc.,
 * within the codebase. size_t isn't necessary, as the maximum offset
 * is only parm::PAGE_SIZE
 */
typedef uint16_t PageOffset;

/* A unique identifier for a frame within a buffer or cache */
typedef int32_t FrameId;

/*
 * A unique timestamp for use in MVCC concurrency control. Currently stored in
 * record headers, but not used by anything.
 */
typedef uint32_t Timestamp;
const Timestamp TIMESTAMP_MIN = 0;
const Timestamp TIMESTAMP_MAX = UINT32_MAX;

/*
 * Invalid values for various IDs. Used throughout the code base to indicate
 * uninitialized values and error conditions.
 */
const PageNum INVALID_PNUM = 0;
const FrameId INVALID_FRID = -1;

typedef ssize_t level_index; /* -1 indicates the buffer */
constexpr level_index buffer_level_idx = -1;
constexpr level_index invalid_level_idx = -2;

typedef ssize_t shard_index; /* -1 indicates "all" shards on a level */
constexpr shard_index all_shards_idx = -1;
constexpr shard_index invalid_shard_idx = -2;

struct ShardID {
  level_index level_idx;
  shard_index shard_idx;

  friend bool operator==(const ShardID &shid1, const ShardID &shid2) {
    return shid1.level_idx == shid2.level_idx &&
           shid1.shard_idx == shid2.shard_idx;
  }
};

constexpr ShardID invalid_shard = {invalid_level_idx, invalid_shard_idx};
constexpr ShardID buffer_shid = {buffer_level_idx, all_shards_idx};

enum class ReconstructionType {
  Invalid, /* placeholder type */
  Flush, /* a flush of the buffer into L0 */
  Merge, /* the merging of shards in two seperate levels */
  Append, /* adding a shard directly to a level */
  Compact /* the merging of shards on one level */
};

typedef struct ReconstructionTask {
  std::vector<ShardID> sources = {};
  level_index target = 0;
  size_t reccnt = 0;
  ReconstructionType type = ReconstructionType::Invalid;

  void add_shard(ShardID shard, size_t cnt) {
    sources.push_back(shard);
    reccnt += cnt;
  }

} ReconstructionTask;

class ReconstructionVector {
public:
  ReconstructionVector() : total_reccnt(0) {}

  ~ReconstructionVector() = default;

  ReconstructionTask operator[](size_t idx) { return m_tasks[idx]; }

  void add_reconstruction(std::vector<ShardID> shards, level_index target,
                          size_t reccnt, ReconstructionType type) {

    m_tasks.push_back({std::move(shards), target, reccnt, type});
    
  }

  void add_reconstruction(level_index source, level_index target,
                          size_t reccnt, ReconstructionType type) {
    m_tasks.push_back({{{source, all_shards_idx}}, target, reccnt});
    total_reccnt += reccnt;
  }

  void add_reconstruction(ReconstructionTask task) { m_tasks.push_back(task); }

  size_t get_total_reccnt() { return total_reccnt; }

  size_t size() { return m_tasks.size(); }

private:
  std::vector<ReconstructionTask> m_tasks;
  size_t total_reccnt;
};

enum class DeletePolicy { TOMBSTONE, TAGGING };

} // namespace de
