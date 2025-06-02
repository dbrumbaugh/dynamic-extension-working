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

  auto data = read_vector_file<Rec, W2V_SIZE>(d_fname, n);

  size_t buffer_size = 1000;
  std::vector<size_t> policies = {0, 1};

  std::vector<size_t> thread_counts = {8};
  std::vector<double> modifiers = {0};
  std::vector<size_t> scale_factors = {2, 4, 8}; 
  std::vector<size_t> knn_sizes = {1, 10, 100, 1000};

  reccnt = n;

  std::vector<std::vector<QP>> query_sets;
  for (auto k : knn_sizes) {
    query_sets.push_back(read_knn_queries<QP>(q_fname, k, 100));
  }

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

          /* load structure */
          size_t warmup = n;
          for (size_t k = 0; k < warmup; k++) {
            while (!extension->insert(data[k])) {
              usleep(1);
            }
          }

          extension->await_version();

          idx.store(warmup);

          extension->await_version();


          TIMER_INIT();
          size_t total = 0;
          for (size_t l=0; l<query_sets.size(); l++) {
              TIMER_START();
              for (size_t f=0; f<query_sets[l].size()*10; f++) {
                  auto q = query_sets[l][f%10];
                  auto res = extension->query(std::move(q));
                  total += res.get().size();
              }
              TIMER_STOP();
              auto query_latency = (TIMER_RESULT()) / (10*query_sets[l].size());
              fprintf(stdout, "%ld\t%ld\t", knn_sizes[l], query_latency);
          }

          fprintf(stdout, "\n");
          fprintf(stderr, "%ld\n", total);
          fflush(stdout);
          delete extension;
        }
      }
    }
  }

  fflush(stderr);
}
