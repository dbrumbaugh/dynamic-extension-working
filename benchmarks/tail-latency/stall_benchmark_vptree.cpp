/*
 *
 */

#include <cstdlib>
#define ENABLE_TIMER
#define DE_PRINT_SHARD_COUNT
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
                             de::FIFOScheduler>
    Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE,
                            de::FIFOScheduler>
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

void insert_thread(Ext *extension, std::vector<Rec> *records, size_t start_idx,
                   size_t stop_idx, gsl_rng *rng) {
  TIMER_INIT();

  for (size_t i = start_idx; i < stop_idx; i++) {
    TIMER_START();
    while (!extension->insert((*records)[i], rng)) {
      usleep(1);
    }
    TIMER_STOP();

    fprintf(stdout, "I\t%ld\n", TIMER_RESULT());
  }
}

void usage(char *progname) {
  fprintf(stderr, "%s reccnt datafile rate_limit policy\n", progname);
}

int main(int argc, char **argv) {

  if (argc < 5) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  size_t n = atol(argv[1]);
  std::string d_fname = std::string(argv[2]);
  double rate_limit = std::atof(argv[3]);
  size_t pol = std::atol(argv[4]);
  assert(pol >= 0 && pol <= 6);

  auto data = read_vector_file<Rec, 300>(d_fname, n);

  size_t buffer_size = 1000;
  size_t scale_factor = 8;
  double modifier = 0;
  size_t insert_threads = 1;
  size_t internal_thread_cnt = 32;
  reccnt = n;

  gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);

  auto policy = get_policy<Shard, Q>(scale_factor, buffer_size, pol, n, modifier);
  auto config = Conf(std::move(policy));
  config.recon_enable_maint_on_flush = true;
  config.recon_maint_disabled = false;
  // config.buffer_flush_trigger = 4000;
  config.maximum_threads = internal_thread_cnt;

  g_thrd_cnt = internal_thread_cnt;

  total_insert_time.store(0);
  total_query_time.store(0);
  total_query_count.store(0);

  auto extension = new Ext(std::move(config), rate_limit);

  /* warmup structure w/ 30% of records */
  size_t warmup = .3 * n;
  for (size_t k = 0; k < warmup; k++) {
    while (!extension->insert(data[k])) {
      usleep(1);
    }
  }

  extension->await_version();

  idx.store(warmup);

  std::thread i_thrds[insert_threads];

  size_t per_insert_thrd = (n - warmup) / insert_threads;
  size_t start = warmup;

  for (size_t i = 0; i < insert_threads; i++) {
    i_thrds[i] = std::thread(insert_thread, extension, &data, start,
                             start + per_insert_thrd, rng);
    start += per_insert_thrd;
  }

  for (size_t i = 0; i < insert_threads; i++) {
    i_thrds[i].join();
  }

  inserts_done.store(true);
  inserts_done.store(false);
  delete extension;

  fflush(stderr);
}
