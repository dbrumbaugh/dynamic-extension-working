/*
 *
 */

#include "benchmark_types.h"
#include <cstdlib>
#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "file_util.h"
#include "framework/DynamicExtension.h"
#include "framework/interface/Record.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "framework/scheduling/SerialScheduler.h"
#include "framework/util/Configuration.h"
#include "query/knn.h"
#include "shard/VPTree.h"
#include "standard_benchmarks.h"
#include "util/types.h"

#include "framework/reconstruction/FixedShardCountPolicy.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef Word2VecRec Rec;
typedef de::VPTree<Rec> Shard;
typedef de::knn::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE,
                             de::SerialScheduler>
    Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE,
                            de::SerialScheduler>
    Conf;

std::atomic<size_t> idx;
std::atomic<bool> inserts_done = false;

ssize_t query_ratio = 0;

std::atomic<size_t> total_res = 0;
size_t reccnt = 0;

size_t g_thrd_cnt = 0;

std::atomic<size_t> total_insert_time = 0;
std::atomic<size_t> total_insert_count = 0;
std::atomic<size_t> total_query_time = 0;
std::atomic<size_t> total_query_count = 0;

void query_thread(Ext *extension, std::vector<QP> *queries) {
  TIMER_INIT();
  while (!inserts_done.load()) {
    total_query_count.fetch_add(1);
    auto q_idx = rand() % queries->size();

    auto q = (*queries)[q_idx];

    TIMER_START();
    auto res = extension->query(std::move(q)).get();
    TIMER_STOP();

    total_query_time.fetch_add(TIMER_RESULT());
    total_res.fetch_add(res.size());
  }
}

void insert_thread(Ext *extension, std::vector<Rec> *records, size_t start_idx,
                   size_t stop_idx) {
  TIMER_INIT();

  TIMER_START();
  for (size_t i = start_idx; i < stop_idx; i++) {
    while (!extension->insert((*records)[i])) {
      usleep(1);
    }
  }

  TIMER_STOP();
  total_insert_time.fetch_add(TIMER_RESULT());
}

void usage(char *progname) {
  fprintf(stderr, "%s reccnt datafile queryfile\n", progname);
}

int main(int argc, char **argv) {

  if (argc < 4) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  size_t n = atol(argv[1]);
  std::string d_fname = std::string(argv[2]);
  std::string q_fname = std::string(argv[3]);

  //auto data = read_sosd_file<Rec>(d_fname, n);
  //auto queries = read_range_queries<QP>(q_fname, .0001);
  //auto queries =read_sosd_point_lookups<QP>(q_fname, 1);

  auto data = read_vector_file<Rec, W2V_SIZE>(d_fname, n);
  auto queries = read_knn_queries<QP>(q_fname, 100, 1);

  size_t buffer_size = 1000;
  std::vector<size_t> policies = {0, 1};

  std::vector<size_t> thread_counts = {8};
  std::vector<double> modifiers = {0};
  std::vector<size_t> scale_factors = {2, 4, 6, 8, 16, 32, 128}; 

  size_t insert_threads = 1;
  size_t query_threads = 1;

  reccnt = n;

  for (auto pol : policies) {
    for (auto internal_thread_cnt : thread_counts) {
      for (auto mod : modifiers) {
        for (auto sf : scale_factors) {
          auto policy = get_policy<Shard, Q>(sf, buffer_size, pol, n, mod);
          auto config = Conf(std::move(policy));
          config.recon_enable_maint_on_flush = true;
          config.recon_maint_disabled = false;
          config.buffer_flush_trigger = config.buffer_size;
          config.maximum_threads = internal_thread_cnt;

          g_thrd_cnt = internal_thread_cnt;

          total_insert_time.store(0);
          total_query_time.store(0);
          total_query_count.store(0);

          auto extension = new Ext(std::move(config));

          /* warmup structure w/ 10% of records */
          size_t warmup = .3 * n;
          for (size_t k = 0; k < warmup; k++) {
            while (!extension->insert(data[k])) {
              usleep(1);
            }
          }

          extension->await_version();

          idx.store(warmup);

          std::thread i_thrds[insert_threads];
          std::thread q_thrds[query_threads];

          size_t per_insert_thrd = (n - warmup) / insert_threads;
          size_t start = warmup;

          for (size_t i = 0; i < insert_threads; i++) {
            i_thrds[i] = std::thread(insert_thread, extension, &data, start,
                                     start + per_insert_thrd);
            start += per_insert_thrd;
          }

          for (size_t i = 0; i < insert_threads; i++) {
            i_thrds[i].join();
          }

          inserts_done.store(true);

          extension->await_version();

          /* run some queries to "warm up" the cache */
          for (size_t i=0; i<queries.size()*5; i++) {
            auto q_idx = i % queries.size();
            auto q = queries[q_idx];
            auto res = extension->query(std::move(q)).get();
            total_res.fetch_add(res.size());
          }

          total_query_count.store(5000);
          TIMER_INIT();
          TIMER_START();
          for (size_t i=0; i<total_query_count; i++) {
            auto q_idx = i % queries.size();
            auto q = queries[q_idx];
            auto res = extension->query(std::move(q)).get();
            total_res.fetch_add(res.size());
          }
          TIMER_STOP();
          total_query_time.store(TIMER_RESULT());
          
          fprintf(stderr, "%ld\n", total_res.load());

          size_t insert_tput =
              ((double)(n - warmup) / (double)total_insert_time) * 1e9;
          size_t query_lat = (double)total_query_time.load() /
                             (double)total_query_count.load();

          fprintf(stdout, "%ld\t%ld\t%ld\t%lf\t%ld\t%ld\t%ld\t%ld\n", internal_thread_cnt, pol, sf,
                  mod, extension->get_height(), extension->get_shard_count(),
                  insert_tput, query_lat);
          //extension->print_scheduler_statistics();
          //extension->print_scheduler_query_data();
          //extension->print_structure();
          fflush(stdout);

          total_res.store(0);
          inserts_done.store(false);
          delete extension;
        }
      }
    }
  }

  fflush(stderr);
}
