/*
 * include/framework/DynamicExtension.h
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
#include <mutex>
#include <vector>

#include "framework/interface/Scheduler.h"
#include "framework/reconstruction/ReconstructionPolicy.h"
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

  typedef std::shared_ptr<VersionType> version_ptr;
  typedef size_t version_id;
  static constexpr size_t INVALID_VERSION = 0;
  static constexpr size_t INITIAL_VERSION = 1;

public:
  /**
   * Create a new Dynamized version of a data structure, supporting
   * inserts and, possibly, deletes. The following parameters are used
   * for configuration of the structure,
   * @param buffer_low_watermark The number of records that can be
   *        inserted before a buffer flush is initiated
   *
   * @param buffer_high_watermark The maximum buffer capacity, inserts
   *        will begin to fail once this number is reached, until the
   *        buffer flush has completed. Has no effect in single-threaded
   *        operation
   *
   * @param scale_factor The rate at which the capacity of levels
   *        grows; should be at least 2 for reasonable performance
   *
   * @param memory_budget Unused at this time
   *
   * @param thread_cnt The maximum number of threads available to the
   *        framework's scheduler for use in answering queries and
   *        performing compactions and flushes, etc.
   */
  DynamicExtension(ConfType &&config) : m_config(std::move(config)) {
    m_buffer =
        std::make_unique<BufferType>(m_config.buffer_flush_trigger, m_config.buffer_size);

    m_sched = std::make_unique<SchedType>(m_config.maximum_memory_usage,
                                          m_config.maximum_threads);
    m_active_version.store(
        std::make_shared<VersionType>(INITIAL_VERSION, std::make_unique<StructureType>(), m_buffer.get(), 0));
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
   *  has reached the high water mark; in this case, the insert should be
   *  retried when the buffer has flushed. The record will be immediately
   *  visible inside the index upon the return of this function.
   *
   *  @param rec The record to be inserted
   *
   *  @return 1 on success, 0 on failure (in which case the insert should
   *          be retried)
   */
  int insert(const RecordType &rec) { return internal_append(rec, false); }

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
   *          failure means that hte record to be deleted could not be
   *          found in the index, and should *not* be retried.
   */
  int erase(const RecordType &rec) {
    // FIXME: delete tagging will require a lot of extra work to get
    //        operating "correctly" in a concurrent environment.

    /*
     * Get a view on the buffer *first*. This will ensure a stronger
     * ordering than simply accessing the buffer directly, but is
     * not *strictly* necessary.
     */
    if constexpr (D == DeletePolicy::TAGGING) {
      static_assert(std::same_as<SchedType, SerialScheduler>,
                    "Tagging is only supported in single-threaded operation");

      auto view = m_buffer->get_buffer_view();
      auto version = get_active_version();
      if (version->get_mutable_structure()->tagged_delete(rec)) {
        return 1;
      }

      /*
       * the buffer will take the longest amount of time, and
       * probably has the lowest probability of having the record,
       * so we'll check it last.
       */
      return view.delete_record(rec);
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
    return get_active_version()->get_structure()->get_height();
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
    auto version = get_active_version();
    auto t = m_buffer->get_memory_usage() +
             version->get_structure()->get_memory_usage();

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
  // FIXME: switch this over to std::unique_ptr
  ShardType *
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

    ShardType *flattened = new ShardType(shards);

    for (auto shard : shards) {
      delete shard;
    }

    return flattened;
  }

  /*
   * Blocks until the specified version id becomes active. If no version
   * id is provided, wait for the newest pending version to be installed.
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
      vid = m_version_counter.load() - 1;
    }

    /* versions signal on m_version_advance_cv when they activate */
    while (m_active_version.load()->get_id() < vid) {
      std::unique_lock lk(m_version_advance_mtx);
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

private:
  ConfType m_config;

  std::unique_ptr<SchedType> m_sched;
  std::unique_ptr<BufferType> m_buffer;

  size_t m_core_cnt;
  std::atomic<int> m_next_core;

  ReconPolicyType const *m_recon_policy;

  /* versioning + concurrency variables */
  std::atomic<size_t> m_version_counter;
  std::atomic<std::shared_ptr<VersionType>> m_active_version;

  std::condition_variable m_version_advance_cv;
  std::mutex m_version_advance_mtx;

  alignas(64) std::atomic<bool> m_scheduling_reconstruction;

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
    extension->SetThreadAffinity();

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
      /* we first construct a shard from the buffer */
      auto buffview = args->version->get_buffer();
      args->version->set_next_buffer_head(buffview.get_tail());
      auto new_shard = std::make_shared<ShardType>(std::move(buffview));

      /*
       * Flushes already know their version id. To avoid needing to
       * do any update reconciliation between structures, they wait
       * until the version directly preceeding them has been installed,
       * and only then take a copy of the structure.
       */
      extension->await_version(args->version->get_id() - 1);
      StructureType *structure =
          extension->get_active_version()->get_structure()->copy();

      /* add the newly created shard to the structure copy */
      structure->append_l0(new_shard);

      /* set this version's structure to the newly created one */
      args->version->set_structure(std::unique_ptr<StructureType>(structure));
    }

    /* perform all of the reconstructions */
    StructureType *structure = args->version->get_mutable_structure();
    for (size_t i = 0; i < args->tasks.size(); i++) {
      structure->perform_reconstruction(args->tasks[i]);
    }

    /*
     * if there isn't already a version id on the new version (i.e., the
     * reconstruction isn't a flush), generate one.
     */
    if (args->version->get_id() == INVALID_VERSION) {
      args->version->set_id(extension->m_version_counter.fetch_add(1));
    }

    /* advance the index to the newly finished version */
    extension->install_new_version(args->version);

    /* manually delete the argument object */
    delete args;
  }

  static void async_query(void *arguments) {
    auto *args = (QueryArgs<ShardType, QueryType, DynamicExtension> *)arguments;

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
  version_ptr create_version() {
    size_t version_id = m_version_counter.fetch_add(1);
    auto active_version = get_active_version();
    std::shared_ptr<VersionType> new_version =
        std::make_shared<VersionType>(version_id, nullptr, m_buffer.get(), active_version->get_buffer().get_head());

    return new_version;
  }

  /*
   * Create a new version without an assigned version number, but with
   * a copy of the extension structure. This is for use with background
   * reconstructions, where the underlying structure is manipulated, but
   * no version number is claimed until the version is activated, to
   * prevent blocking buffer flushes.
   */
  version_ptr create_version(std::unique_ptr<StructureType> structure) {
    auto active_version = get_active_version();
    version_ptr new_version =
        std::make_shared<VersionType>(INVALID_VERSION, std::move(structure), m_buffer.get(), active_version->get_buffer().get_head());

    return new_version;
  }

  void install_new_version(version_ptr new_version) {
    assert(new_version->get_structure());
    assert(new_version->get_id() != INVALID_VERSION);

    /* wait until our turn to install the new version */
    await_version(new_version->get_id() - 1);

    auto old = get_active_version();

    // FIXME: implement this interface
    // new_version->merge_changes_from(old.load().get());

    /*
     * Only one version can have a given number, so we are safe to
     * directly assign here--nobody else is going to change it out from
     * under us.
     */
    m_active_version.store(new_version);

    /*
     * My understanding is that you don't *really* need this mutex for
     * safety in modern C++ when sending the signal. But I'll grab it
     * anyway out of an abundance of caution. I doubt this will be a
     * major bottleneck.
     */
    auto lk = std::unique_lock(m_version_advance_mtx);
    m_version_advance_cv.notify_all();
  }

  StructureType *create_scratch_structure() {
    return get_active_version()->get_structure()->copy();
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
    auto new_version = create_version();

    auto *args = new ReconstructionArgs<ShardType, QueryType>();
    args->version = new_version;
    args->tasks = m_recon_policy->get_flush_tasks(args->version.get());
    args->extension = this;
    args->priority = ReconstructionPriority::FLUSH;

    /*
     * NOTE: args is deleted by the reconstruction job, so shouldn't be
     * freed here
     */
    m_sched->schedule_job(reconstruction, m_buffer->get_high_watermark(), args,
                         RECONSTRUCTION);

    if (m_config.recon_enable_maint_on_flush) {
      schedule_maint_reconstruction(false);
    }

    end_reconstruction_scheduling();
  }

  void schedule_maint_reconstruction(bool take_reconstruction_lock = true) {

    if (take_reconstruction_lock) {
      begin_reconstruction_scheduling();
    }

    // FIXME: memory management issue here?
    auto new_version = create_version(std::unique_ptr<StructureType>(m_active_version.load()->get_structure()->copy()));

    auto *args = new ReconstructionArgs<ShardType, QueryType>();
    args->version = new_version;
    args->tasks = m_recon_policy->get_reconstruction_tasks(args->version.get());
    args->extension = this;
    args->priority = ReconstructionPriority::MAINT;

    /*
     * NOTE: args is deleted by the reconstruction job, so shouldn't be
     * freed here
     */
    m_sched->schedule_job(reconstruction, m_buffer->get_high_watermark(), args,
                         RECONSTRUCTION);

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

  int internal_append(const RecordType &rec, bool ts) {
    if (m_buffer->is_at_low_watermark()) {
      auto old = false;
      if (m_scheduling_reconstruction.compare_exchange_strong(old, true)) {
        schedule_flush();
      }
    }

    /* this will fail if the HWM is reached and return 0 */
    return m_buffer->append(rec, ts);
  }

//#ifdef _GNU_SOURCE
#if 0
  void SetThreadAffinity() {
    if constexpr (std::same_as<SchedType, SerialScheduler>) {
      return;
    }

    int core = m_next_core.fetch_add(1) % m_core_cnt;
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
#else
  void SetThreadAffinity() {}
#endif
};
} // namespace de
