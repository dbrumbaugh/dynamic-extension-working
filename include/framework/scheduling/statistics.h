/*
 * include/framework/scheduling/statistics.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * This is a stub for a statistics tracker to be used in scheduling. It
 * currently only tracks simple aggregated statistics, but should be
 * updated in the future for more fine-grained statistics. These will be
 * used for making scheduling decisions and predicting the runtime of a
 * given job.
 */
#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace de {

class SchedulerStatistics {
private:
  enum class EventType { QUEUED, SCHEDULED, STARTED, FINISHED };

  struct Event {
    size_t id;
    EventType type;
    std::chrono::system_clock::time_point time;

    Event(size_t id, EventType type)
        : id(id), type(type), time(std::chrono::high_resolution_clock::now()) {}
  };

  struct JobInfo {
    size_t id;
    size_t size;
    size_t type;

    JobInfo(size_t id, size_t size, size_t type) : id(id), size(size), type(type) {}
  };

public:
  SchedulerStatistics() = default;
  ~SchedulerStatistics() = default;

  void job_queued(size_t id, size_t type, size_t size) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_jobs.insert({id, {id, size, type}});
    m_event_log.emplace_back(id, EventType::QUEUED);
  }

  void job_scheduled(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::SCHEDULED);
  }

  void job_begin(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::STARTED);
  }

  void job_complete(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::FINISHED);
  }

  /* FIXME: This is just a temporary approach */
  void log_time_data(size_t length, size_t type) {
    assert(type == 1 || type == 2 || type == 3);

    if (type == 1) {
      m_type_1_cnt.fetch_add(1);
      m_type_1_total_time.fetch_add(length);

      if (length > m_type_1_largest_time) {
        m_type_1_largest_time.store(length);
      }
    } else if (type == 2) {
      m_type_2_cnt.fetch_add(1);
      m_type_2_total_time.fetch_add(length);

      if (length > m_type_2_largest_time) {
        m_type_2_largest_time.store(length);
      }
    }
  }

  void print_statistics() {
    int64_t total_queue_time = 0;
    int64_t max_queue_time = 0;
    int64_t min_queue_time = INT64_MAX;

    int64_t total_runtime = 0;
    int64_t max_runtime = 0;
    int64_t min_runtime = INT64_MAX;

    int64_t query_cnt = 0;

    /* dumb brute force approach; there are a million better ways to do this */
    size_t i = 0;
    for (auto &job : m_jobs) {
      std::chrono::system_clock::time_point queue_time;
      std::chrono::system_clock::time_point schedule_time;
      std::chrono::system_clock::time_point start_time;
      std::chrono::system_clock::time_point stop_time;

      /* just look at queries for now */
      if (job.second.type == 1) {
        for (auto &event : m_event_log) {
          if (event.id == job.first) {
            switch (event.type) {
              case EventType::QUEUED:
                queue_time = event.time;
                i++;
                break;
              case EventType::FINISHED:
                stop_time = event.time;
                i++;
                break;
              case EventType::SCHEDULED:
                schedule_time = event.time;
                i++;
                break;
              case EventType::STARTED:
                start_time = event.time;
                i++;
                break;
            }
          }
        }
      }
      /* event wasn't fully logged, so we'll skip it */
      if (i != 4) {
        i=0;
        continue;
      }
      i=0;

      auto time_in_queue = std::chrono::duration_cast<std::chrono::nanoseconds>(schedule_time - queue_time).count();
      auto runtime =  std::chrono::duration_cast<std::chrono::nanoseconds>(stop_time - start_time).count();

      total_queue_time += time_in_queue;
      total_runtime += runtime;

      if (time_in_queue > max_queue_time) {
        max_queue_time = time_in_queue;
      }

      if (time_in_queue < min_queue_time) {
        min_queue_time = time_in_queue;
      }

      if (runtime > max_runtime) {
        max_runtime = runtime;
      }

      if (runtime < min_runtime) {
        min_runtime = runtime;
      }

      query_cnt++;
    }

    if (query_cnt == 0) {
      return;
    }

    int64_t average_queue_time = total_queue_time / query_cnt;
    int64_t average_runtime = total_runtime / query_cnt;

    fprintf(stdout, "Average Query Scheduling Delay: %ld\t Min Scheduling Delay: %ld\t Max Scheduling Delay: %ld\n", average_queue_time, min_queue_time, max_queue_time);
    fprintf(stdout, "Average Query Latency: %ld\t\t Min Query Latency: %ld\t Max Query Latency: %ld\n", average_runtime, min_runtime, max_runtime);
  }

private:
  std::mutex m_mutex;
  std::unordered_map<size_t, JobInfo> m_jobs;
  std::vector<Event> m_event_log;

  std::atomic<size_t> m_type_1_cnt;
  std::atomic<size_t> m_type_1_total_time;

  std::atomic<size_t> m_type_2_cnt;
  std::atomic<size_t> m_type_2_total_time;

  std::atomic<size_t> m_type_1_largest_time;
  std::atomic<size_t> m_type_2_largest_time;
};
} // namespace de
