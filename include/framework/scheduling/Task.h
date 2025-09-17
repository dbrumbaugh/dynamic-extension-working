/*
 * include/framework/scheduling/Task.h
 *
 * Copyright (C) 2023-2025 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * An abstraction to represent a job to be scheduled. Currently the
 * supported task types are queries and merges. Based on the current plan,
 * simple buffer inserts will likely also be made into a task at some
 * point.
 *
 */
#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <condition_variable>

#include "framework/scheduling/Version.h"
#include "framework/scheduling/statistics.h"
#include "util/types.h"

namespace de {

enum class ReconstructionPriority {
  FLUSH = 0,
  CMPCT = 1,
  MAINT = 2  
};

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
struct ReconstructionArgs {
  typedef typename ShardType::RECORD RecordType;
  std::shared_ptr<Version<ShardType, QueryType>> version;
  ReconstructionVector tasks;
  void *extension;
  ReconstructionPriority priority;
  size_t initial_version;
  long predicted_runtime;
};

template <ShardInterface S, QueryInterface<S> Q, typename DE> struct QueryArgs {
  std::promise<typename Q::ResultType> result_set;
  typename Q::Parameters query_parms;
  DE *extension;
};

typedef std::function<void(void *)> Job;

struct Task {
  Task(size_t size, size_t ts, Job job, void *args, size_t type = 0,
       SchedulerStatistics *stats = nullptr, std::mutex *lk = nullptr, std::condition_variable *cv=nullptr)
      : m_job(job), m_size(size), m_timestamp(ts), m_args(args), m_type(type),
        m_stats(stats), m_lk(lk), m_cv(cv) {}

  Job m_job;
  size_t m_size;
  size_t m_timestamp;
  void *m_args;
  size_t m_type;
  SchedulerStatistics *m_stats;
  std::mutex *m_lk;
  std::condition_variable *m_cv;

  friend bool operator<(const Task &self, const Task &other) {
    return self.m_timestamp < other.m_timestamp;
  }

  friend bool operator>(const Task &self, const Task &other) {
    return self.m_timestamp > other.m_timestamp;
  }

  void operator()(size_t thrd_id) {
    if (m_stats) {
      m_stats->job_begin(m_timestamp);
    }

    m_job(m_args);

    if (m_stats) {
      m_stats->job_complete(m_timestamp);
    }

    if (m_lk) {
      m_lk->unlock();
    }

    if (m_cv) {
      m_cv->notify_all();
    }
  }
};

} // namespace de
