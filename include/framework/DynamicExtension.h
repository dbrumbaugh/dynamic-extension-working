/*
 * include/framework/DynamicExtension.h
 *
 * Copyright (C) 2023-2025 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <cstdio>
#include <gsl/gsl_rng.h>
#include <mutex>
#include <set>
#include <vector>

#include "framework/interface/Scheduler.h"
#include "framework/reconstruction/ReconstructionPolicy.h"
#include "framework/scheduling/LockManager.h"
#include "framework/scheduling/SerialScheduler.h"

#include "framework/scheduling/Task.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/structure/MutableBuffer.h"

#include "framework/util/Configuration.h"

#include "framework/scheduling/Version.h"
#include "util/types.h"

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
          DeletePolicy D = DeletePolicy::TAGGING,
          SchedulerInterface SchedType = de::SerialScheduler>
class DynamicExtension {
private:
  /* convenience typedefs for commonly used types within the class */
  typedef typename ShardType::RECORD RecordType;
  typedef MutableBuffer<RecordType> BufferType;
  typedef ExtensionStructure<ShardType, QueryType> StructureType;
  typedef Version<ShardType, QueryType> VersionType;
  typedef BufferView<RecordType> BufferViewType;
  typedef ReconstructionPolicy<ShardType, QueryType> ReconPolicyType;
  typedef DEConfiguration<ShardType, QueryType, D, SchedType> ConfType;

  typedef typename QueryType::Parameters Parameters;
  typedef typename QueryType::LocalQuery LocalQuery;
  typedef typename QueryType::LocalQueryBuffer BufferQuery;
  typedef typename QueryType::LocalResultType LocalResult;
  typedef typename QueryType::ResultType QueryResult;

  static constexpr size_t QUERY = 1;
  static constexpr size_t RECONSTRUCTION = 2;
  static constexpr size_t FLUSH = 3;

  typedef std::shared_ptr<VersionType> version_ptr;
  typedef size_t version_id;
  static constexpr size_t INVALID_VERSION = 0;
  static constexpr size_t INITIAL_VERSION = 1;

public:
  /**
   * Create a new Dynamized version of a data structure, supporting
   * inserts and, possibly, deletes. The following parameters are used
   * for configuration of the structure,
   * @param config A configuration object detailing the requested values
   *        for various configuration parameters in the system. See
   *        include/framework/util/Configuration.h for details.
   */
  DynamicExtension(ConfType &&config) : m_config(std::move(config)) {
    m_buffer = std::make_unique<BufferType>(m_config.buffer_flush_trigger,
                                            m_config.buffer_size);

    m_sched = std::make_unique<SchedType>(m_config.maximum_memory_usage,
                                          m_config.maximum_threads);
    m_active_version.store(std::make_shared<VersionType>(
        INITIAL_VERSION, std::make_unique<StructureType>(), m_buffer.get(), 0));

    m_version_counter = INITIAL_VERSION;
    m_preempt_version = INVALID_VERSION;
    m_insertion_rate.store(1.0);
    assert(m_config.recon_policy);
  }

  /**
   *  Destructor for DynamicExtension. Will block until the completion of
   *  any outstanding version transition, shut down the scheduler, and free
   *  all currently allocated shards, buffers, etc., by calling their
   *  destructors.
   */
  ~DynamicExtension() {
    /* let any in-flight version transitions finish */
    await_version();

    /* shutdown the scheduler */
    m_sched->shutdown();
  }

  /**
   *  Inserts a record into the index. Returns 1 if the insert succeeds,
   *  and 0 if it fails. Inserts may fail if the DynamicExtension's buffer
   *  has reached the high water mark, or if rate limiting is triggered;
   *  in either case, the insert should be retried after a brief pause.
   *  The record will be immediately visible inside the index upon the
   *  successful return of this function.
   *
   *  @param rec The record to be inserted
   *
   *  @param rng An optional random number generator for use with insertion
   *         rate limiting. Can be left as nullptr if the feature is not
   *         desired.
   *
   *  @return 1 on success, 0 on failure. A failure can occur if either
   *          the buffer is full and is in the process of flushing, or if
   *          the insertion is rejected by the rate limiter. In either
   *          case, the insert should be retried after a brief pause.
   */
  int insert(const RecordType &rec, gsl_rng *rng = nullptr) {
    return internal_append(rec, false, rng);
  }

  /**
   *  Erases a record from the index, according to the DeletePolicy
   *  template parameter. Returns 1 on success and 0 on failure. The
   *  equality comparison operator of RecordType is used to identify
   *  the record to be deleted.
   *
   *  Deletes behave differently, depending on the DeletionPolicy. For
   *  Tombstone deletes, a tombstone record will be inserted into the
   *  index. The presence of the deleted record is not checked first, so
   *  deleting a record that does not exist will result in an unnecessary
   *  tombstone record being written.
   *
   *  Deletes using Tagging will perform a point lookup for the record to
   *  be removed, and mark it as deleted in its header.
   *
   *  @param rec The record to be deleted. The argument to this function
   *         should compare equal to the record to be deleted.
   *
   *  @return 1 on success, and 0 on failure. For tombstone deletes, a
   *          failure will occur if the insert fails due to the buffer
   *          being full, and can be retried. For tagging deletes, a
   *          failure means that the record to be deleted could not be
   *          found in the index, and should *not* be retried.
   */
  int erase(const RecordType &rec) {
    // FIXME: delete tagging will require a lot of extra work to get
    //        operating "correctly" in a concurrent environment.

    // FIXME: should integrate with the rate limiter for tombstone
    //        deletes.

    /*
     * Get a view on the buffer *first*. This will ensure a stronger
     * ordering than simply accessing the buffer directly, but is
     * not *strictly* necessary.
     */
    if constexpr (D == DeletePolicy::TAGGING) {
      static_assert(std::same_as<SchedType, SerialScheduler>,
                    "Tagging is only supported in single-threaded operation");

      auto version = get_active_version();
      if (version->get_mutable_structure()->tagged_delete(rec)) {
        return 1;
      }

      /*
       * the buffer will take the longest amount of time, and
       * probably has the lowest probability of having the record,
       * so we'll check it last.
       */
      return version->get_buffer().delete_record(rec);
    }

    /*
     * If tagging isn't used, then delete using a tombstone
     */
    return internal_append(rec, true);
  }

  /**
   *  Schedule the execution of a query with specified parameters and
   *  returns a future that can be used to access the results. The query
   *  is executed asynchronously.
   *  @param parms An rvalue reference to the query parameters.
   *
   *  @return A future, from which the query results can be retrieved upon
   *          query completion
   */
  std::future<QueryResult> query(Parameters &&parms) {
    return schedule_query(std::move(parms));
  }

  /**
   *  Determine the number of records (including tagged records and
   *  tombstones) currently within the framework. This number is used for
   *  determining when and how reconstructions occur.
   *
   *  @return The number of records within the index
   */
  size_t get_record_count() {
    auto version = get_active_version();
    auto t = version->get_buffer().get_record_count() +
             version->get_structure()->get_record_count();

    return t;
  }

  /**
   *  Returns the number of tombstone records currently within the
   *  index. This function can be called when tagged deletes are used,
   *  but will always return 0 in that case.
   *
   *  @return The number of tombstone records within the index
   */
  size_t get_tombstone_count() {
    auto version = get_active_version();
    auto t = version->get_buffer().get_tombstone_count() +
             version->get_structure()->get_tombstone_count();

    return t;
  }

  /**
   *  Get the number of levels within the framework. This count will
   *  include any empty levels, but will not include the buffer. Note that
   *  this is *not* the same as the number of shards when tiering is used,
   *  as each level can contain multiple shards in that case.
   *
   *  @return The number of levels within the index
   */
  size_t get_height() {
    return get_active_version()->get_structure()->get_height() - 1;
  }

  /**
   * Get the number of non-empty shards within the index.
   *
   * @return The number of non-empty shards within the index
   */
  size_t get_shard_count() {
    return get_active_version()->get_structure()->get_shard_count();
  }

  /**
   *  Get the number of bytes of memory allocated across the framework for
   *  storing records and associated index information (i.e., internal
   *  ISAM tree nodes). This includes memory that is allocated but
   *  currently unused in the buffer, or in shards themselves
   *  (overallocation due to delete cancellation, etc.).
   *
   *  @return The number of bytes of memory used for shards (as reported by
   *          ShardType::get_memory_usage) and the buffer by the index.
   */
  size_t get_memory_usage() {
    auto t = m_buffer->get_memory_usage() +
             get_active_version()->get_structure()->get_memory_usage();

    return t;
  }

  /**
   *  Get the number of bytes of memory allocated across the framework for
   *  auxiliary structures. This can include bloom filters, aux
   *  hashtables, etc.
   *
   *  @return The number of bytes of memory used for auxilliary structures
   *          (as reported by ShardType::get_aux_memory_usage) by the index.
   */
  size_t get_aux_memory_usage() {
    return get_active_version()->get_structure()->get_aux_memory_usage();
  }

  /**
   *  Create a new single Shard object containing all of the records
   *  within the framework (buffer and shards).
   *
   *  @param await_reconstruction_completion Specifies whether the currently
   *         active state of the index should be used to create the shard
   *         (false), or if shard construction should wait for any active
   *         reconstructions to finish first (true). Default value of false.
   *
   *  @return A new shard object, containing a copy of all records within
   *          the index. Ownership of this object is transfered to the
   *          caller.
   */
  std::unique_ptr<ShardType>
  create_static_structure(bool await_reconstruction_completion = false) {
    if (await_reconstruction_completion) {
      await_version();
    }

    auto version = get_active_version();
    auto structure = version->get_structure();
    std::vector<const ShardType *> shards;

    if (structure->get_level_vector().size() > 0) {
      for (int i = structure->get_level_vector().size() - 1; i >= 0; i--) {
        if (structure->get_level_vector()[i] &&
            structure->get_level_vector()[i]->get_record_count() > 0) {
          shards.emplace_back(
              structure->get_level_vector()[i]->get_combined_shard());
        }
      }
    }

    /*
     * construct a shard from the buffer view. We'll hold the view
     * for as short a time as possible: once the records are exfiltrated
     * from the buffer, there's no reason to retain a hold on the view's
     * head pointer any longer
     */
    {
      auto bv = version->get_buffer();
      if (bv.get_record_count() > 0) {
        shards.emplace_back(new ShardType(std::move(bv)));
      }
    }

    auto flattened = std::make_unique<ShardType>(shards);

    for (auto shard : shards) {
      delete shard;
    }

    return flattened;
  }

  /**
   * Blocks until the specified version id becomes active. If no version
   * id is provided, wait for the newest pending version to be installed.
   *
   * @param vid The version id to wait for. This function will block until
   *        the active version is greater than or equal to vid. If no
   *        vid is provided, then the function will block until the
   *        largest in-flight version number at the time of the function
   *        call is made active.
   *
   * NOTE: this method will return once the specified version has been
   *       installed, but does not guarantee that the specified version
   *       is the currently active one when it returns. It is possible
   *       that the active version upon the return of this method is newer
   *       than the one requested.
   */
  void await_version(version_id vid = INVALID_VERSION) {
    /*
     * versions are assigned by fetch and add on the counter, so the
     * newest assigned version number will be one less than the value
     * of the counter
     */
    if (vid == INVALID_VERSION) {
      vid = m_version_counter.load();
    }

    /* versions signal on m_version_advance_cv when they activate */
    std::unique_lock lk(m_version_advance_mtx);
    while (m_active_version.load()->get_id() < vid) {
      m_version_advance_cv.wait(lk);
    }

    return;
  }

  /**
   *  Verify that the currently active version of the index does not
   *  violate tombstone proportion invariants. Exposed for unit-testing
   *  purposes.
   *
   *  @return Returns true if the tombstone proportion invariant is
   *  satisfied, and false if it is not.
   */
  bool validate_tombstone_proportion() {
    return get_active_version()->get_structure()->validate_tombstone_proportion(
        m_config.maximum_delete_proportion);
  }

  /**
   * Calls SchedType::print_statistics, which should write a report of
   * scheduler performance statistics to stdout.
   */
  void print_scheduler_statistics() const { m_sched->print_statistics(); }

  void print_scheduler_query_data() const { m_sched->print_query_time_data(); }

  /**
   * Writes a schematic view of the currently active structure to
   * stdout. Each level is on its own line, and each shard is represented.
   */
  void print_structure() {
    auto ver = get_active_version();
    auto bv = ver->get_buffer();

    fprintf(stdout, "[B]:\t(%ld)\n", bv.get_record_count());
    ver->get_structure()->print_structure();
  }

private:
  ConfType m_config;

  std::unique_ptr<SchedType> m_sched;
  std::unique_ptr<BufferType> m_buffer;

  size_t m_core_cnt;
  std::atomic<int> m_next_core;

  /* versioning + concurrency variables */
  alignas(64) std::atomic<size_t> m_version_counter;
  alignas(64) std::atomic<std::shared_ptr<VersionType>> m_active_version;

  alignas(64) std::condition_variable m_version_advance_cv;
  alignas(64) std::mutex m_version_advance_mtx;

  alignas(64) LockManager m_lock_mngr;

  alignas(64) std::atomic<size_t> m_preempt_version;

  alignas(64) std::atomic<bool> m_scheduling_reconstruction;

  alignas(64) std::atomic<double> m_insertion_rate;

  bool restart_query(QueryArgs<ShardType, QueryType, DynamicExtension> *args,
                     size_t version) {
    if (version <= m_preempt_version.load()) {
      // fprintf(stderr, "[I] Preempted query on version %ld, restarting...\n",
      // version);
      m_sched->schedule_job(async_query, 0, (void *)args, QUERY);
      return true;
    }

    return false;
  }

  void preempt_queries() {
    size_t vers = m_active_version.load()->get_id() - 1;
    if (vers != m_preempt_version.load()) {
      m_preempt_version.store(vers);
      // fprintf(stderr, "[I] Initiating query preemption on version %ld\n",
      // vers);
    }
  }

  void enforce_delete_invariant(VersionType *version) {
    auto structure = version->get_structure();
    auto compactions = structure->get_compaction_tasks();

    while (compactions.size() > 0) {

      /* schedule a compaction */
      ReconstructionArgs<ShardType, QueryType> *args =
          new ReconstructionArgs<ShardType, QueryType>();
      args->version = version;
      args->merges = compactions;
      args->extension = this;
      args->compaction = true;
      /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed
       * here */

      auto wait = args->result.get_future();

      m_sched->schedule_job(reconstruction, 0, args, RECONSTRUCTION);

      /* wait for compaction completion */
      wait.get();

      /* get a new batch of compactions to perform, if needed */
      compactions = structure->get_compaction_tasks();
    }
  }

  static void reconstruction(void *arguments) {
    auto args = (ReconstructionArgs<ShardType, QueryType> *)arguments;
    auto extension = (DynamicExtension *)args->extension;
    extension->set_thread_affinity();

    static std::atomic<size_t> cnt = 0;
    size_t recon_id = cnt.fetch_add(1);

    size_t new_head = 0;
    std::vector<reconstruction_results<ShardType>> reconstructions;

    /*
     * For "normal" flushes, the task vector should be empty, so this is
     * all that will happen. Piggybacking internal reconstructions off
     * the flush WILL bottleneck the system, but is left in place to
     * allow the traditional policies (LEVELING, TIERING, etc.) to be
     * emulated within the new system.
     *
     * Background reconstructions will not have a priority level of
     * FLUSH, and will already have a structure present. As a result,
     * this code will be bypassed in that case.
     */
    if (args->priority == ReconstructionPriority::FLUSH) {
      // fprintf(stderr, "[I] Running flush (%ld)\n", recon_id);
      // fprintf(stderr, "[I]\t Assigned Version %ld (%ld)\n",
      // args->version->get_id(), recon_id);

      /* we first construct a shard from the buffer */
      auto buffview = args->version->get_buffer();
      assert(buffview.get_tail() != buffview.get_head());
      new_head = buffview.get_tail();

      // fprintf(stderr, "\t[I] Current Buffer Head:\t%ld (%ld)\n",
      // buffview.get_head(), recon_id);

      reconstruction_results<ShardType> flush_recon;
      flush_recon.target_level = 0;
      flush_recon.new_shard = std::make_shared<ShardType>(std::move(buffview));

      reconstructions.push_back(flush_recon);

      /*
       * Eager policies need access to the flushed shard immediately, so
       * we add it to the current structure. This gives such policies
       * access to it within their own reconstructions later.
       *
       * This is necessary for Leveling and BSM specifically. Tiering-based
       * policies can ignore this shard (by explicitly specifying the
       * shards in L0 to use), or use it (by using the "all_shards_idx"
       * shard id).
       */
      args->version->get_mutable_structure()->append_shard(
          flush_recon.new_shard, args->version->get_id(),
          flush_recon.target_level);

      /* advance the buffer head for a flush */
      bool success = false;
      size_t failure_cnt = 0;
      while (!success) {
        success = args->version->advance_buffer_head(new_head);
        if (!success) {
          failure_cnt++;
          usleep(1);
          // fprintf(stderr, "\t[I] Buffer head advance blocked on %ld (%ld)\n",
          // args->version->get_id(), recon_id);

          if (failure_cnt >=
              extension->m_config.buffer_flush_query_preemption_trigger) {
            extension->preempt_queries();

            if (failure_cnt > 500000) {
              // fprintf(stderr, "[C] Critical failure. Hung on version: %ld
              // (%ld)\n", extension->m_buffer->debug_get_old_head(), recon_id);
            }
          }
        }
      }
      // fprintf(stderr, "\t[I] Buffer head advanced to:\t%ld (%ld)\n",
      // new_head, recon_id);
    } else {
      // fprintf(stderr, "[I] Running background reconstruction (%ld)\n",
      // recon_id);
    }

    /* perform all of the reconstructions */
    auto structure = args->version->get_structure();
    assert(structure);

    // fprintf(stderr, "\t[I] Pre-reconstruction L0 Size\t%ld (%ld)\n",
    // structure->get_level_vector()[0]->get_shard_count(), recon_id);

    for (size_t i = 0; i < args->tasks.size(); i++) {
      reconstructions.emplace_back(
          structure->perform_reconstruction(args->tasks[i]));
    }

    /*
     * if there isn't already a version id on the new version (i.e., the
     * reconstruction isn't a flush), generate one.
     */
    if (args->version->get_id() == INVALID_VERSION) {
      assert(args->priority == ReconstructionPriority::MAINT);
      args->version->set_id(extension->m_version_counter.fetch_add(1) + 1);
      // fprintf(stderr, "\t[I] Reconstruction version assigned %ld (%ld)\n",
      // args->version->get_id(), recon_id);
    } else {
      assert(args->priority == ReconstructionPriority::FLUSH);
    }

    /* wait for our opportunity to install the updates */
    extension->await_version(args->version->get_id() - 1);

    // size_t old_reccnt = args->version->get_structure()->get_record_count();

    /*
     * this version *should* have an ID one less than the version we are
     * currently constructing, and should be fully stable (i.e., only the
     * buffer tail can change)
     */
    auto active_version = extension->get_active_version();
    assert(active_version->get_id() == args->version->get_id() - 1);

    /* get a fresh copy of the structure from the current version */
    args->version->set_structure(std::move(std::unique_ptr<StructureType>(
        active_version->get_structure()->copy())));

    size_t cur_reccnt = args->version->get_structure()->get_record_count();

    /* apply our updates to the copied structure (adding/removing shards) */
    for (auto recon : reconstructions) {
      auto grow = args->version->get_mutable_structure()->append_shard(
          recon.new_shard, args->version->get_id(), recon.target_level);
      args->version->get_mutable_structure()->delete_shards(
          recon.source_shards);
      if (grow) {
        extension->m_lock_mngr.add_lock();
      }
    }

    size_t new_reccnt = args->version->get_structure()->get_record_count();

    // fprintf(stderr, "\t[I] Post-reconstruction L0 Size\t%ld (%ld)\n",
    // args->version->get_structure()->get_level_vector()[0]->get_shard_count(),
    // recon_id);

    /* for maintenance reconstructions, advance the buffer head to match the
     * currently active version */
    if (args->priority == ReconstructionPriority::MAINT) {
      args->version->set_buffer(extension->m_buffer.get(),
                                active_version->get_head());
      // fprintf(stderr, "\t[I] Buffer head set to %ld (%ld)\n",
      // active_version->get_head(), recon_id);
      if (new_reccnt != cur_reccnt) {
        fprintf(stderr, "ERROR: invalid reccnt (%ld)\n", recon_id);
      }
    }

    // fprintf(stderr, "\t[I] Record Counts: %ld %ld %ld (%ld)\n", old_reccnt,
    // cur_reccnt, new_reccnt, recon_id);

    /* advance the index to the newly finished version */
    extension->install_new_version(args->version, args->initial_version);

    /* maint reconstructions can now safely release their locks */
    if (args->priority == ReconstructionPriority::MAINT) {
      std::set<size_t> locked_levels;
      for (size_t i = 0; i < args->tasks.size(); i++) {
        for (auto source : args->tasks[i].sources) {
          locked_levels.insert(source.level_idx);
        }
      }

      for (auto level : locked_levels) {
        // fprintf(stderr, "\t[I] releasing lock on %ld (%ld)\n", level,
        // recon_id);
        extension->m_lock_mngr.release_lock(level, args->version->get_id());
      }
    }

    if (args->priority == ReconstructionPriority::FLUSH) {
      // fprintf(stderr, "\t[I] releasing lock on buffer (%ld)\n", recon_id);
      extension->m_lock_mngr.release_buffer_lock();
    }

    // fprintf(stderr, "[I] Reconstruction to Version %ld Finished (%ld)\n",
    // args->version->get_id(), recon_id);

    /* manually delete the argument object */
    delete args;
  }

  static void async_query(void *arguments) {
    auto *args = (QueryArgs<ShardType, QueryType, DynamicExtension> *)arguments;
    args->extension->set_thread_affinity();

    auto version = args->extension->get_active_version();

    auto buffer = version->get_buffer();
    auto vers = version->get_structure();
    auto *parms = &(args->query_parms);

    /* create initial buffer query */
    auto buffer_query = QueryType::local_preproc_buffer(&buffer, parms);

    /* create initial local queries */
    std::vector<std::pair<ShardID, ShardType *>> shards;
    std::vector<LocalQuery *> local_queries =
        vers->get_local_queries(shards, parms);

    /* process local/buffer queries to create the final version */
    QueryType::distribute_query(parms, local_queries, buffer_query);

    /* execute the local/buffer queries and combine the results into output */
    QueryResult output;
    do {
      /*
       * for query preemption--check if the query should be restarted
       * to prevent blocking buffer flushes for too long
       */
      if (args->extension->restart_query(args, version->get_id())) {
        /* clean up memory allocated for temporary query objects */
        delete buffer_query;
        for (size_t i = 0; i < local_queries.size(); i++) {
          delete local_queries[i];
        }
        return;
      }

      std::vector<LocalResult> query_results(shards.size() + 1);
      for (size_t i = 0; i < query_results.size(); i++) {
        if (i == 0) { /* execute buffer query */
          query_results[i] = QueryType::local_query_buffer(buffer_query);
        } else { /*execute local queries */
          query_results[i] = QueryType::local_query(shards[i - 1].second,
                                                    local_queries[i - 1]);
        }

        /* end query early if EARLY_ABORT is set and a result exists */
        if constexpr (QueryType::EARLY_ABORT) {
          if (query_results[i].size() > 0)
            break;
        }
      }

      /*
       * combine the results of the local queries, also translating
       * from LocalResultType to ResultType
       */
      QueryType::combine(query_results, parms, output);

      /* optionally repeat the local queries if necessary */
    } while (QueryType::repeat(parms, output, local_queries, buffer_query));

    /* return the output vector to caller via the future */
    args->result_set.set_value(std::move(output));

    /* clean up memory allocated for temporary query objects */
    delete buffer_query;
    for (size_t i = 0; i < local_queries.size(); i++) {
      delete local_queries[i];
    }

    delete args;
  }

  version_ptr get_active_version() { return m_active_version.load(); }

  /*
   * Create a new version with an assigned version number, but without
   * an assigned copy of the structure. Intended for use in flushing,
   * where the structure will be copied from the currently active version
   * at the time it is activated, but the version number must be claimed
   * early to minimize activation blocking.
   */
  version_ptr create_version_flush(std::unique_ptr<StructureType> structure) {
    size_t version_id = m_version_counter.fetch_add(1) + 1;
    // fprintf(stderr, "[I] Flush version assigned (%ld)\n", version_id);
    auto active_version = get_active_version();
    std::shared_ptr<VersionType> new_version = std::make_shared<VersionType>(
        version_id, std::move(structure), m_buffer.get(),
        active_version->get_buffer().get_head());

    return new_version;
  }

  /*
   * Create a new version without an assigned version number, but with
   * a copy of the extension structure. This is for use with background
   * reconstructions, where the underlying structure is manipulated, but
   * no version number is claimed until the version is activated, to
   * prevent blocking buffer flushes.
   */
  version_ptr create_version_maint(std::unique_ptr<StructureType> structure) {
    auto active_version = get_active_version();
    version_ptr new_version = std::make_shared<VersionType>(
        INVALID_VERSION, std::move(structure), nullptr, 0);

    return new_version;
  }

  void install_new_version(version_ptr new_version,
                           size_t old_active_version_id) {
    assert(new_version->valid());

    // fprintf(stderr, "\t[I] Installing version %ld\n", new_version->get_id());

    /* wait until our turn to install the new version */
    await_version(new_version->get_id() - 1);

    auto lk = std::unique_lock(m_version_advance_mtx);
    /*
     * Only one version can have a given number, so we are safe to
     * directly assign here--nobody else is going to change it out from
     * under us.
     */
    m_active_version.store(new_version);
    m_version_advance_cv.notify_all();

    // fprintf(stderr, "\t[I] Installed version %ld\n", new_version->get_id());
  }

  void begin_reconstruction_scheduling() {
    bool cur_val;
    do {
      cur_val = m_scheduling_reconstruction.load();
    } while (
        cur_val == true &&
        !m_scheduling_reconstruction.compare_exchange_strong(cur_val, true));
  }

  void end_reconstruction_scheduling() {
    /* no need for any other sync here, this thread has an implicit lock */
    m_scheduling_reconstruction.store(false);
  }

  void schedule_flush() {
    begin_reconstruction_scheduling();

    if (!m_lock_mngr.take_buffer_lock()) {
      end_reconstruction_scheduling();
      return;
    }

    /*
     * Double check that we actually need to flush. I was seeing some
     * flushes running on empty buffers--somehow it seems like a new
     * flush was getting scheduled immediately after another one finished,
     * when it wasn't necessary. This should prevent that from happening.
     *
     * A bit of a kludge, but it *should* work
     */
    if (!m_buffer->is_at_low_watermark()) {
      m_lock_mngr.release_buffer_lock();
      end_reconstruction_scheduling();
      return;
    }

    auto active_version = m_active_version.load();

    auto *args = new ReconstructionArgs<ShardType, QueryType>();
    args->version = create_version_flush(std::unique_ptr<StructureType>(
        active_version->get_structure()->copy()));
    args->tasks = m_config.recon_policy->get_flush_tasks(active_version.get());
    args->extension = this;
    args->priority = ReconstructionPriority::FLUSH;
    args->initial_version = INVALID_VERSION;

    // fprintf(stderr, "[I] Scheduling flush (%ld)\n", args->version->get_id());

    /*
     * NOTE: args is deleted by the reconstruction job, so shouldn't be
     * freed here
     */
    m_sched->schedule_job(reconstruction, m_buffer->get_high_watermark(), args,
                          FLUSH);
    // fprintf(stderr, "[I] Finished scheduling flush (%ld)\n",
    // args->version->get_id());

    if (m_config.recon_enable_maint_on_flush) {
      schedule_maint_reconstruction(false);
    }

    end_reconstruction_scheduling();
  }

  void schedule_maint_reconstruction(bool take_reconstruction_lock = true) {

    if (m_config.recon_maint_disabled) {
      return;
    }

    if (take_reconstruction_lock) {
      begin_reconstruction_scheduling();
    }

    // fprintf(stderr, "[I] Scheduling maintenance\n");

    auto active_version = m_active_version.load();
    auto reconstructions = m_config.recon_policy->get_reconstruction_tasks(
        active_version.get(), m_lock_mngr);

    for (auto &recon : reconstructions) {
      /*
       * NOTE: args is deleted by the reconstruction job, so shouldn't be
       * freed here
       */
      auto *args = new ReconstructionArgs<ShardType, QueryType>();
      args->version = create_version_maint(std::unique_ptr<StructureType>(
          active_version->get_structure()->copy()));
      args->tasks = std::move(recon);
      args->extension = this;
      args->priority = ReconstructionPriority::MAINT;
      args->initial_version = active_version->get_id();
      m_sched->schedule_job(reconstruction, args->tasks.get_total_reccnt(),
                            args, RECONSTRUCTION);
    }

    if (take_reconstruction_lock) {
      end_reconstruction_scheduling();
    }

    return;
  }

  std::future<QueryResult> schedule_query(Parameters &&query_parms) {
    auto args = new QueryArgs<ShardType, QueryType, DynamicExtension>();
    args->extension = this;
    args->query_parms = std::move(query_parms);
    auto result = args->result_set.get_future();

    m_sched->schedule_job(async_query, 0, (void *)args, QUERY);

    return result;
  }

  int internal_append(const RecordType &rec, bool ts, gsl_rng *rng = nullptr) {
    if (m_buffer->is_at_low_watermark() && !m_lock_mngr.is_buffer_locked()) {
      schedule_flush();
    }

    /*
     * Insertion rate limiting is handled by Bernoulli Sampling to determine
     * whether a given insert succeeds or not. If an insert is allowed to
     * happen, a buffer append will be attempted (this might still fail,
     * though). Otherwise, the insert will not be attempted and should
     * be retried. If the insertion rate is unity, then no probabilistic
     * filtering will occur.
     */
    if (m_insertion_rate.load() < 1 && rng) {
      auto p = gsl_rng_uniform(rng);
      if (p > m_insertion_rate.load()) {
        return false;
      }
    }

    /* this will fail if the HWM is reached and return 0 */
    return m_buffer->append(rec, ts);
  }

//#ifdef _GNU_SOURCE
  void set_thread_affinity() {
    if constexpr (std::same_as<SchedType, SerialScheduler>) {
      return;
    }

    int core = m_next_core.fetch_add(1) % m_config.physical_core_count;
    cpu_set_t mask;
    CPU_ZERO(&mask);

    switch (core % 2) {
    case 0:
      // 0 |-> 0
      // 2 |-> 2
      // 4 |-> 4
      core = core + 0;
      break;
    case 1:
      // 1 |-> 28
      // 3 |-> 30
      // 5 |-> 32
      core = (core - 1) + m_core_cnt;
      break;
    }
    CPU_SET(core, &mask);
    ::sched_setaffinity(0, sizeof(mask), &mask);
  }
  /*
#else
  void set_thread_affinity() {}
#endif
*/
};
} // namespace de
