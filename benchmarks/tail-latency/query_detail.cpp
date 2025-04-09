/*
 *
 */

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
#include "query/pointlookup.h"
#include "shard/ISAMTree.h"
#include "standard_benchmarks.h"
#include "util/types.h"

#include "framework/reconstruction/FixedShardCountPolicy.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"

typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::ISAMTree<Rec> Shard;
typedef de::pl::Query<Shard> Q;
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
  //auto queries = read_range_queries<QP>(q_fname, .0001);
  auto queries =read_sosd_point_lookups<QP>(q_fname, 100);

  size_t buffer_size = 8000;

  auto policy = get_policy<Shard, Q>(4, buffer_size);
  auto config = Conf(std::move(policy));
  auto extension = new Ext(std::move(config));

  /* warmup structure w/ 10% of records */
  size_t warmup = n;
  for (size_t k = 0; k < warmup; k++) {
    while (!extension->insert(data[k])) {
      usleep(1);
    }
  }

  extension->print_structure();

  TIMER_INIT();
  
  TIMER_START();
  for (size_t i=0; i<10000; i++) {
      auto q_idx = rand() % queries.size();
      auto q = queries[q_idx];
      auto res = extension->query(std::move(q)).get();
      total_res.fetch_add(res.size());
  }
  TIMER_STOP();
  
  fprintf(stdout, "End-to-end Query Latency:\t%ld\n", TIMER_RESULT() / 10000);

  /* run query */

  delete extension;

  policy = get_policy<Shard, Q>(4, buffer_size, 1);
  config = Conf(std::move(policy));
  extension = new Ext(std::move(config));

  /* warmup structure w/ 10% of records */
  for (size_t k = 0; k < warmup; k++) {
    while (!extension->insert(data[k])) {
      usleep(1);
    }
  }

  extension->print_structure();

  TIMER_START();
  for (size_t i=0; i<10000; i++) {
    auto q_idx = rand() % queries.size();
    auto q = queries[q_idx];
    auto res = extension->query(std::move(q)).get();
    total_res.fetch_add(res.size());
  }
  TIMER_STOP();
  
  fprintf(stdout, "End-to-end Query Latency:\t%ld\n", TIMER_RESULT() / 10000);

  delete extension;

  fprintf(stderr, "%ld\n", total_res.load());

  fflush(stderr);
}
