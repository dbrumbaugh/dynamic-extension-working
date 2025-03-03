/*
 *
 */

#include <cstdlib>
#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/scheduling/SerialScheduler.h"
#include "framework/util/Configuration.h"
#include "util/types.h"
#include "file_util.h"
#include "framework/DynamicExtension.h"
#include "framework/interface/Record.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "query/rangecount.h"
#include "shard/TrieSpline.h"
#include "standard_benchmarks.h"

#include "framework/reconstruction/FixedShardCountPolicy.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"

typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::TrieSpline<Rec> Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE,
                             de::FIFOScheduler>
    Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE,
                            de::FIFOScheduler>
    Conf;

std::atomic<size_t> idx;
std::atomic<bool> inserts_done = false;

ssize_t query_ratio = 8;

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
      total_res.fetch_add(res);
  }
}

void insert_thread(Ext *extension, std::vector<Rec> *records, size_t start_idx, size_t stop_idx) {
  TIMER_INIT();

  TIMER_START();

  for (size_t i=start_idx; i<stop_idx; i++) {
    //extension->print_structure();

    while (!extension->insert((*records)[i])) {
      usleep(1);
      //fprintf(stderr, "[D] Failed to insert\n");
    }

    //fprintf(stderr, "[D] Record inserted\n");

    if (extension->get_record_count() != i + 1) {
      fprintf(stderr, "[E]: invalid record count %ld %ld\n", extension->get_record_count(), i+1);
      extension->print_structure();
      exit(EXIT_FAILURE);
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

  auto data = read_sosd_file<Rec>(d_fname, n);
  auto queries = read_range_queries<QP>(q_fname, .0001);

  std::vector<size_t> sfs = {8}; //, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
  size_t buffer_size = 8000;
  std::vector<size_t> policies = {
      5
  };

  std::vector<size_t> thread_counts = {8};

  size_t insert_threads = 1;
  size_t query_threads = 16;

  reccnt = n;

  for (auto pol : policies) {
    for (auto internal_thread_cnt : thread_counts) {
      auto policy = get_policy<Shard, Q>(sfs[0], buffer_size, pol, n);
      auto config = Conf(std::move(policy));
      config.recon_enable_maint_on_flush = true;
      config.recon_maint_disabled = false;
      //config.buffer_flush_trigger = 4000;
      config.maximum_threads = internal_thread_cnt;

      g_thrd_cnt = internal_thread_cnt;

      total_insert_time.store(0);
      total_query_time.store(0);
      total_query_count.store(0);
      
      auto extension = new Ext(std::move(config));

      /* warmup structure w/ 10% of records */
      size_t warmup = 0 * n;
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

      for (size_t i=0; i<insert_threads; i++) {
        i_thrds[i] = std::thread(insert_thread, extension, &data, start, start + per_insert_thrd);
        start += per_insert_thrd;
      }

      for (size_t i=0; i<query_threads; i++) {
        q_thrds[i] = std::thread(query_thread, extension, &queries);
      }

      for (size_t i=0; i<insert_threads; i++) {
        i_thrds[i].join();
      }

      inserts_done.store(true);

      for (size_t i=0; i<query_threads; i++) {
        q_thrds[i].join();
      }

      fprintf(stderr, "%ld\n", total_res.load());

      size_t insert_tput = ((double)(n - warmup) / (double) total_insert_time) *1e9;
      size_t query_lat = (double) total_query_time.load() / (double) total_query_count.load();

      fprintf(stdout, "%ld\t%ld\t%ld\n", internal_thread_cnt, insert_tput, query_lat);
      fflush(stdout);

      total_res.store(0);
      inserts_done.store(false);
      delete extension;
    }
  }

  fflush(stderr);
}
