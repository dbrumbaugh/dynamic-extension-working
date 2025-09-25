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
#include <cmath>
#include <cstdint>
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

    std::chrono::system_clock::time_point queue_time;
    std::chrono::system_clock::time_point scheduled_time;
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point stop_time;

    JobInfo(size_t id, size_t size, size_t type)
        : id(id), size(size), type(type) {}

    int64_t time_in_queue() {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(
                 scheduled_time - queue_time)
          .count();
    }

    int64_t runtime() {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(stop_time -
                                                                  start_time)
          .count();
    }

    int64_t end_to_end_time() {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(stop_time -
                                                                  queue_time)
          .count();
    }
  };

public:
  SchedulerStatistics() = default;
  ~SchedulerStatistics() = default;

  void job_queued(size_t id, size_t type, size_t size) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_jobs.insert({id, {id, size, type}});
    m_event_log.emplace_back(id, EventType::QUEUED);
    m_job_times_set = false;
  }

  void job_scheduled(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::SCHEDULED);
    m_job_times_set = false;
  }

  void job_begin(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::STARTED);
    m_job_times_set = false;
  }

  void job_complete(size_t id) {
    std::unique_lock<std::mutex> lk(m_mutex);
    m_event_log.emplace_back(id, EventType::FINISHED);
    m_job_times_set = false;
  }

  void print_statistics() {
    std::unique_lock<std::mutex> lk(m_mutex);
    update_job_data_from_log();

    int64_t total_queue_time = 0;
    int64_t max_queue_time = 0;
    int64_t min_queue_time = INT64_MAX;

    int64_t total_runtime = 0;
    int64_t max_runtime = 0;
    int64_t min_runtime = INT64_MAX;

    int64_t query_cnt = 0;

    size_t worst_query = 0;
    size_t first_query = UINT64_MAX;

    for (auto &job : m_jobs) {
      if (job.second.type != 1) {
        fprintf(stdout, "%ld %ld %ld %ld\n", job.second.id, job.second.size,
                job.second.runtime(), job.second.runtime() / (job.second.size));
      }

      if (job.first < first_query) {
        first_query = job.first;
      }

      total_queue_time += job.second.time_in_queue();
      total_runtime += job.second.runtime();

      if (job.second.time_in_queue() > max_queue_time) {
        max_queue_time = job.second.time_in_queue();
      }

      if (job.second.time_in_queue() < min_queue_time) {
        min_queue_time = job.second.time_in_queue();
      }

      if (job.second.runtime() > max_runtime) {
        max_runtime = job.second.runtime();
        worst_query = job.first;
      }

      if (job.second.runtime() < min_runtime) {
        min_runtime = job.second.runtime();
      }

      query_cnt++;
    }

    int64_t average_queue_time = (query_cnt) ? total_queue_time / query_cnt : 0;
    int64_t average_runtime = (query_cnt) ? total_runtime / query_cnt : 0;

    /* calculate standard deviations */
    int64_t queue_deviation_sum = 0;
    int64_t runtime_deviation_sum = 0;
    for (auto &job : m_jobs) {
      if (job.second.type != 1) {
        continue;
      }

      queue_deviation_sum +=
          std::pow(job.second.time_in_queue() - average_queue_time, 2);
      runtime_deviation_sum +=
          std::pow(job.second.runtime() - average_runtime, 2);
    }

    int64_t queue_stddev =
        (query_cnt) ? std::sqrt(queue_deviation_sum / query_cnt) : 0;
    int64_t runtime_stddev =
        (query_cnt) ? std::sqrt(runtime_deviation_sum / query_cnt) : 0;

    fprintf(stdout, "Query Count: %ld\tWorst Query: %ld\tFirst Query: %ld\n",
            query_cnt, worst_query, first_query);
    fprintf(stdout,
            "Average Query Scheduling Delay: %ld\t Min Scheduling Delay: %ld\t "
            "Max Scheduling Delay: %ld\tStandard Deviation: %ld\n",
            average_queue_time, min_queue_time, max_queue_time, queue_stddev);
    fprintf(stdout,
            "Average Query Latency: %ld\t\t Min Query Latency: %ld\t Max Query "
            "Latency: %ld\tStandard Deviation: %ld\n",
            average_runtime, min_runtime, max_runtime, runtime_stddev);
  }

  void print_query_time_data() {
    std::unique_lock<std::mutex> lk(m_mutex);
    update_job_data_from_log();

    for (auto &job : m_jobs) {
      if (job.second.type == 1) {
        fprintf(stdout, "%ld\t%ld\t%ld\n", job.second.time_in_queue(),
                job.second.runtime(), job.second.end_to_end_time());
      }
    }
  }

private:
  std::mutex m_mutex;
  std::unordered_map<size_t, JobInfo> m_jobs;
  std::vector<Event> m_event_log;
  bool m_job_times_set;

  void update_job_data_from_log() {
    if (m_job_times_set) {
      return;
    }

    /*
     * these updates could be made when the time point is recorded, but I
     * want to keep as much of this off the critical path as possible. Any
     * work done here won't affect performance.
     */
    for (auto &event : m_event_log) {
      if (!m_jobs.contains(event.id)) {
        continue;
      }

      auto &job = m_jobs.at(event.id);
      switch (event.type) {
      case EventType::QUEUED:
        job.queue_time = event.time;
        break;
      case EventType::FINISHED:
        job.stop_time = event.time;
        break;
      case EventType::SCHEDULED:
        job.scheduled_time = event.time;
        break;
      case EventType::STARTED:
        job.start_time = event.time;
        break;
      }
    }

    m_job_times_set = true;
  }
};
} // namespace de
