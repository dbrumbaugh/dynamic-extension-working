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
#include <vector>

#include "framework/interface/Scheduler.h"
#include "framework/scheduling/SerialScheduler.h"

#include "framework/structure/ExtensionStructure.h"
#include "framework/structure/MutableBuffer.h"

#include "framework/scheduling/Epoch.h"
#include "framework/util/Configuration.h"

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
          LayoutPolicy L = LayoutPolicy::TEIRING,
          DeletePolicy D = DeletePolicy::TAGGING,
          SchedulerInterface SchedType = SerialScheduler>
class DynamicExtension {
  /* for unit testing purposes */
public:
    LayoutPolicy Layout = L;

private:
  /* convenience typedefs for commonly used types within the class */
  typedef typename ShardType::RECORD RecordType;
  typedef MutableBuffer<RecordType> Buffer;
  typedef ExtensionStructure<ShardType, QueryType, L> Structure;
  typedef Epoch<ShardType, QueryType, L> _Epoch;
  typedef BufferView<RecordType> BufView;

  typedef typename QueryType::Parameters Parameters;
  typedef typename QueryType::LocalQuery LocalQuery;
  typedef typename QueryType::LocalQueryBuffer BufferQuery;
  typedef typename QueryType::LocalResultType LocalResult;
  typedef typename QueryType::ResultType QueryResult;
  

  static constexpr size_t QUERY = 1;
  static constexpr size_t RECONSTRUCTION = 2;

  struct epoch_ptr {
    _Epoch *epoch;
    size_t refcnt;
  };

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
  DynamicExtension(size_t buffer_low_watermark, size_t buffer_high_watermark,
                   size_t scale_factor, size_t memory_budget = 0,
                   size_t thread_cnt = 16)
      : m_scale_factor(scale_factor), m_max_delete_prop(1),
        m_sched(memory_budget, thread_cnt),
        m_buffer(new Buffer(buffer_low_watermark, buffer_high_watermark)),
        m_core_cnt(thread_cnt), m_next_core(0), m_epoch_cnt(0) {
    if constexpr (L == LayoutPolicy::BSM) {
      assert(scale_factor == 2);
    }

    auto vers =
        new Structure(buffer_high_watermark, m_scale_factor, m_max_delete_prop);
    m_current_epoch.store({new _Epoch(0, vers, m_buffer, 0), 0});
    m_previous_epoch.store({nullptr, 0});
    m_next_epoch.store({nullptr, 0});
  }

  /**
   *  Destructor for DynamicExtension. Will block until the completion of
   *  any outstanding epoch transition, shut down the scheduler, and free
   *  all currently allocated shards, buffers, etc., by calling their
   *  destructors.
   */
  ~DynamicExtension() {

    /* let any in-flight epoch transition finish */
    await_next_epoch();

    /* shutdown the scheduler */
    m_sched.shutdown();

    /* delete all held resources */
    delete m_next_epoch.load().epoch;
    delete m_current_epoch.load().epoch;
    delete m_previous_epoch.load().epoch;

    delete m_buffer;
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

      auto epoch = get_active_epoch();
      if (epoch->get_structure()->tagged_delete(rec)) {
        end_job(epoch);
        return 1;
      }

      end_job(epoch);

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
  std::future<QueryResult>
  query(Parameters &&parms) {
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
    auto epoch = get_active_epoch();
    auto t = epoch->get_buffer().get_record_count() +
             epoch->get_structure()->get_record_count();
    end_job(epoch);

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
    auto epoch = get_active_epoch();
    auto t = epoch->get_buffer().get_tombstone_count() +
             epoch->get_structure()->get_tombstone_count();
    end_job(epoch);

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
    auto epoch = get_active_epoch();
    auto t = epoch->get_structure()->get_height();
    end_job(epoch);

    return t;
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
    auto epoch = get_active_epoch();
    auto t = m_buffer->get_memory_usage() +
             epoch->get_structure()->get_memory_usage();
    end_job(epoch);

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
    auto epoch = get_active_epoch();
    auto t = epoch->get_structure()->get_aux_memory_usage();
    end_job(epoch);

    return t;
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
  ShardType *
  create_static_structure(bool await_reconstruction_completion = false) {
    if (await_reconstruction_completion) {
      await_next_epoch();
    }

    auto epoch = get_active_epoch();
    auto vers = epoch->get_structure();
    std::vector<ShardType *> shards;

    if (vers->get_levels().size() > 0) {
      for (int i = vers->get_levels().size() - 1; i >= 0; i--) {
        if (vers->get_levels()[i] &&
            vers->get_levels()[i]->get_record_count() > 0) {
          shards.emplace_back(vers->get_levels()[i]->get_combined_shard());
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
      auto bv = epoch->get_buffer();
      if (bv.get_record_count() > 0) {
        shards.emplace_back(new ShardType(std::move(bv)));
      }
    }

    ShardType *flattened = new ShardType(shards);

    for (auto shard : shards) {
      delete shard;
    }

    end_job(epoch);
    return flattened;
  }

  /*
   * If the current epoch is *not* the newest one, then wait for
   * the newest one to become available. Otherwise, returns immediately.
   */
  void await_next_epoch() {
    while (m_next_epoch.load().epoch != nullptr) {
      std::unique_lock<std::mutex> lk(m_epoch_cv_lk);
      m_epoch_cv.wait(lk);
    }
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
    auto epoch = get_active_epoch();
    auto t = epoch->get_structure()->validate_tombstone_proportion();
    end_job(epoch);
    return t;
  }

  /**
   * Calls SchedType::print_statistics, which should write a report of
   * scheduler performance statistics to stdout.
   */
  void print_scheduler_statistics() const { m_sched.print_statistics(); }

private:
  size_t m_scale_factor;
  double m_max_delete_prop;

  SchedType m_sched;
  Buffer *m_buffer;

  size_t m_core_cnt;
  std::atomic<int> m_next_core;
  std::atomic<size_t> m_epoch_cnt;
  
  alignas(64) std::atomic<bool> m_reconstruction_scheduled;

  std::atomic<epoch_ptr> m_next_epoch;
  std::atomic<epoch_ptr> m_current_epoch;
  std::atomic<epoch_ptr> m_previous_epoch;

  std::condition_variable m_epoch_cv;
  std::mutex m_epoch_cv_lk;




  void enforce_delete_invariant(_Epoch *epoch) {
    auto structure = epoch->get_structure();
    auto compactions = structure->get_compaction_tasks();

    while (compactions.size() > 0) {

      /* schedule a compaction */
      ReconstructionArgs<ShardType, QueryType, L> *args =
          new ReconstructionArgs<ShardType, QueryType, L>();
      args->epoch = epoch;
      args->merges = compactions;
      args->extension = this;
      args->compaction = true;
      /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed
       * here */

      auto wait = args->result.get_future();

      m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);

      /* wait for compaction completion */
      wait.get();

      /* get a new batch of compactions to perform, if needed */
      compactions = structure->get_compaction_tasks();
    }
  }

  _Epoch *get_active_epoch() {
    epoch_ptr old, new_ptr;

    do {
      /*
       * during an epoch transition, a nullptr will installed in the
       * current_epoch. At this moment, the "new" current epoch will
       * soon be installed, but the "current" current epoch has been
       * moved back to m_previous_epoch.
       */
      if (m_current_epoch.load().epoch == nullptr) {
        old = m_previous_epoch;
        new_ptr = {old.epoch, old.refcnt + 1};
        if (old.epoch != nullptr &&
            m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
          break;
        }
      } else {
        old = m_current_epoch;
        new_ptr = {old.epoch, old.refcnt + 1};
        if (old.epoch != nullptr &&
            m_current_epoch.compare_exchange_strong(old, new_ptr)) {
          break;
        }
      }
    } while (true);

    assert(new_ptr.refcnt > 0);

    return new_ptr.epoch;
  }

  void advance_epoch(size_t buffer_head) {

    retire_epoch(m_previous_epoch.load().epoch);

    epoch_ptr tmp = {nullptr, 0};
    epoch_ptr cur;
    do {
      cur = m_current_epoch;
    } while (!m_current_epoch.compare_exchange_strong(cur, tmp));

    m_previous_epoch.store(cur);

    // FIXME: this may currently block because there isn't any
    // query preemption yet. At this point, we'd need to either
    // 1) wait for all queries on the old_head to finish
    // 2) kill all queries on the old_head
    // 3) somehow migrate all queries on the old_head to the new
    //    version
    while (!m_next_epoch.load().epoch->advance_buffer_head(buffer_head)) {
      _mm_pause();
    }

    m_current_epoch.store(m_next_epoch);
    m_next_epoch.store({nullptr, 0});

    /* notify any blocking threads that the new epoch is available */
    m_epoch_cv_lk.lock();
    m_epoch_cv.notify_all();
    m_epoch_cv_lk.unlock();
  }

  /*
   * Creates a new epoch by copying the currently active one. The new epoch's
   * structure will be a shallow copy of the old one's.
   */
  _Epoch *create_new_epoch() {
    /*
     * This epoch access is _not_ protected under the assumption that
     * only one reconstruction will be able to trigger at a time. If that
     * condition is violated, it is possible that this code will clone a retired
     * epoch.
     */
    assert(m_next_epoch.load().epoch == nullptr);
    auto current_epoch = get_active_epoch();

    m_epoch_cnt.fetch_add(1);
    m_next_epoch.store({current_epoch->clone(m_epoch_cnt.load()), 0});

    end_job(current_epoch);

    return m_next_epoch.load().epoch;
  }

  void retire_epoch(_Epoch *epoch) {
    /*
     * Epochs with currently active jobs cannot
     * be retired. By the time retire_epoch is called,
     * it is assumed that a new epoch is active, meaning
     * that the epoch to be retired should no longer
     * accumulate new active jobs. Eventually, this
     * number will hit zero and the function will
     * proceed.
     */

    if (epoch == nullptr) {
      return;
    }

    epoch_ptr old, new_ptr;
    new_ptr = {nullptr, 0};
    do {
      old = m_previous_epoch.load();

      /*
       * If running in single threaded mode, the failure to retire
       * an Epoch will result in the thread of execution blocking
       * indefinitely.
       */
      if constexpr (std::same_as<SchedType, SerialScheduler>) {
        if (old.epoch == epoch)
          assert(old.refcnt == 0);
      }

      if (old.epoch == epoch && old.refcnt == 0 &&
          m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
        break;
      }
      usleep(1);

    } while (true);

    delete epoch;
  }

  static void reconstruction(void *arguments) {
    auto args = (ReconstructionArgs<ShardType, QueryType, L> *)arguments;

    ((DynamicExtension *)args->extension)->SetThreadAffinity();
    Structure *vers = args->epoch->get_structure();

    if constexpr (L == LayoutPolicy::BSM) {
      if (args->merges.size() > 0) {
        vers->reconstruction(args->merges[0]);
      }
    } else {
      for (size_t i = 0; i < args->merges.size(); i++) {
        vers->reconstruction(args->merges[i].target,
                             args->merges[i].sources[0]);
      }
    }

    /*
     * we'll grab the buffer AFTER doing the internal reconstruction, so we
     * can flush as many records as possible in one go. The reconstruction
     * was done so as to make room for the full buffer anyway, so there's
     * no real benefit to doing this first.
     */
    auto buffer_view = args->epoch->get_buffer();
    size_t new_head = buffer_view.get_tail();

    /*
     * if performing a compaction, don't flush the buffer, as
     * there is no guarantee that any necessary reconstructions
     * will free sufficient space in L0 to support a flush
     */
    if (!args->compaction) {
      vers->flush_buffer(std::move(buffer_view));
    }

    args->result.set_value(true);

    /*
     * Compactions occur on an epoch _before_ it becomes active,
     * and as a result the active epoch should _not_ be advanced as
     * part of a compaction
     */
    if (!args->compaction) {
      ((DynamicExtension *)args->extension)->advance_epoch(new_head);
    }

    ((DynamicExtension *)args->extension)
        ->m_reconstruction_scheduled.store(false);

    delete args;
  }

  static void async_query(void *arguments) {
    auto *args = 
      (QueryArgs<ShardType, QueryType, DynamicExtension> *) arguments;

    auto epoch = args->extension->get_active_epoch();

    auto buffer = epoch->get_buffer();
    auto vers = epoch->get_structure();
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

    /* officially end the query job, releasing the pin on the epoch */
    args->extension->end_job(epoch);

    /* clean up memory allocated for temporary query objects */
    delete buffer_query;
    for (size_t i = 0; i < local_queries.size(); i++) {
      delete local_queries[i];
    }

    delete args;
  }

  void schedule_reconstruction() {
    auto epoch = create_new_epoch();

    ReconstructionArgs<ShardType, QueryType, L> *args =
        new ReconstructionArgs<ShardType, QueryType, L>();
    args->epoch = epoch;
    args->merges = epoch->get_structure()->get_reconstruction_tasks(
        m_buffer->get_high_watermark());
    args->extension = this;
    args->compaction = false;
    /* NOTE: args is deleted by the reconstruction job, so shouldn't be freed
     * here */

    m_sched.schedule_job(reconstruction, 0, args, RECONSTRUCTION);
  }

  std::future<QueryResult>
  schedule_query(Parameters &&query_parms) {
    auto args =
        new QueryArgs<ShardType, QueryType, DynamicExtension>();
    args->extension = this;
    args->query_parms = std::move(query_parms);
    auto result = args->result_set.get_future();

    m_sched.schedule_job(async_query, 0, (void *)args, QUERY);

    return result;
  }

  int internal_append(const RecordType &rec, bool ts) {
    if (m_buffer->is_at_low_watermark()) {
      auto old = false;

      if (m_reconstruction_scheduled.compare_exchange_strong(old, true)) {
        schedule_reconstruction();
      }
    }

    /* this will fail if the HWM is reached and return 0 */
    return m_buffer->append(rec, ts);
  }

#ifdef _GNU_SOURCE
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

  void end_job(_Epoch *epoch) {
    epoch_ptr old, new_ptr;

    do {
      if (m_previous_epoch.load().epoch == epoch) {
        old = m_previous_epoch;
        /*
         * This could happen if we get into the system during a
         * transition. In this case, we can just back out and retry
         */
        if (old.epoch == nullptr) {
          continue;
        }

        assert(old.refcnt > 0);

        new_ptr = {old.epoch, old.refcnt - 1};
        if (m_previous_epoch.compare_exchange_strong(old, new_ptr)) {
          break;
        }
      } else {
        old = m_current_epoch;
        /*
         * This could happen if we get into the system during a
         * transition. In this case, we can just back out and retry
         */
        if (old.epoch == nullptr) {
          continue;
        }

        assert(old.refcnt > 0);

        new_ptr = {old.epoch, old.refcnt - 1};
        if (m_current_epoch.compare_exchange_strong(old, new_ptr)) {
          break;
        }
      }
    } while (true);
  }
};
} // namespace de
