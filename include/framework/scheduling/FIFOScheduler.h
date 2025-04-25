/*
 * include/framework/scheduling/FIFOScheduler.h
 *
 * Copyright (C) 2023-2025 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * This scheduler runs just concurrently, using a standard FIFO queue to
 * determine which jobs to run next. If more jobs are scheduled than there
 * are available threads, the excess will stall until a thread becomes
 * available and then run in the order they were received by the scheduler.
 */
#pragma once

#include "framework/scheduling/Task.h"
#include "framework/scheduling/statistics.h"
#include <bits/chrono.h>
#include <chrono>
#include <condition_variable>
#include <thread>

#include "ctpl/ctpl.h"
#include "psu-ds/LockedPriorityQueue.h"

namespace de {

using namespace std::literals::chrono_literals;

class FIFOScheduler {
private:
  static const size_t DEFAULT_MAX_THREADS = 8;

public:
  FIFOScheduler(size_t memory_budget, size_t thread_cnt)
      : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX),
        m_thrd_cnt((thread_cnt) ? thread_cnt : DEFAULT_MAX_THREADS),
        m_used_memory(0), m_used_thrds(0), m_shutdown(false) {
    m_sched_thrd = std::thread(&FIFOScheduler::run, this);
    m_thrd_pool.resize(m_thrd_cnt);
  }

  ~FIFOScheduler() {
    if (!m_shutdown.load()) {
      shutdown();
    }

    m_sched_thrd.join();
    m_flush_thread.join();
  }

  void schedule_job(std::function<void(void *)> job, size_t size, void *args,
                    size_t type = 0) {

    size_t ts = m_counter.fetch_add(1);

    if (type == 3) {
      do_flush(Task(size, ts, job, args, type, &m_stats, &m_flush_lock, &m_cv));
      return;
    }

    std::unique_lock<std::mutex> lk(m_cv_lock);
    m_stats.job_queued(ts, type, size);
    m_task_queue.push(Task(size, ts, job, args, type, &m_stats, nullptr, &m_cv));

    m_cv.notify_all();
  }

  void shutdown() {
    m_shutdown.store(true);
    m_thrd_pool.stop(true);
    m_cv.notify_all();
  }

  void print_statistics() { m_stats.print_statistics(); }
  void print_query_time_data() { m_stats.print_query_time_data(); }

private:
  psudb::LockedPriorityQueue<Task> m_task_queue;

  [[maybe_unused]] size_t m_memory_budget;
  size_t m_thrd_cnt;


  std::atomic<size_t> m_counter;
  std::mutex m_cv_lock;
  std::condition_variable m_cv;
  std::mutex m_queue_lock;

  std::mutex m_flush_lock;
  std::thread m_flush_thread;

  std::thread m_sched_thrd;
  ctpl::thread_pool m_thrd_pool;

  std::atomic<size_t> m_used_memory;
  std::atomic<size_t> m_used_thrds;

  std::atomic<bool> m_shutdown;

  SchedulerStatistics m_stats;

  void schedule_next() {
    auto lk = std::unique_lock<std::mutex>(m_queue_lock);
    assert(m_task_queue.size() > 0);
    auto t = m_task_queue.pop();
    m_stats.job_scheduled(t.m_timestamp);

    m_thrd_pool.push(t);
  }

  void run() {
    std::unique_lock<std::mutex> cv_lock(m_cv_lock);
    do {
      m_cv.wait(cv_lock);

      while (m_task_queue.size() > 0 && m_thrd_pool.n_idle() > 0) {
        schedule_next();
      }
    } while (!m_shutdown.load());
  }

  void do_flush(Task task) {
    m_flush_lock.lock();
    if (m_flush_thread.joinable()) {
      m_flush_thread.join();
    }
    m_flush_thread = std::thread(task, 0);
  }
};

} // namespace de
