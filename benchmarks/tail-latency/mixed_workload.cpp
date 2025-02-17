/*
 *
 */

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

size_t query_ratio = 3;

std::atomic<size_t> total_res = 0;
size_t reccnt = 0;

void operation_thread(Ext *extension, std::vector<QP> *queries,
                      std::vector<Rec> *records) {
  TIMER_INIT();
  while (!inserts_done.load()) {
    auto type = rand() % 10;

    if (type < 8) {
      auto q_idx = rand() % queries->size();

      auto q = (*queries)[q_idx];

      TIMER_START();
      auto res = extension->query(std::move(q)).get();
      TIMER_STOP();

      fprintf(stdout, "Q\t%ld\n", TIMER_RESULT());

      total_res.fetch_add(res);

    } else {
      for (size_t i = 0; i < 1000; i++) {
        auto insert_idx = idx.fetch_add(1);
        if (insert_idx >= reccnt) {
          inserts_done.store(true);
          break;
        }

        TIMER_START();
        while (!extension->insert((*records)[insert_idx])) {
          usleep(1);
        }
        TIMER_STOP();

        fprintf(stdout, "I\t%ld\n", TIMER_RESULT());

        if (idx.load() == reccnt) {
          inserts_done.store(true);
        }
      }
    }
  }
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
      0,
  };

  reccnt = n;

  for (auto pol : policies) {
    for (size_t i = 0; i < sfs.size(); i++) {
      auto policy = get_policy<Shard, Q>(sfs[i], buffer_size, pol, n);
      auto config = Conf(std::move(policy));
      config.recon_enable_maint_on_flush = false;
      config.recon_maint_disabled = true;
      config.buffer_flush_trigger = 4000;

      auto extension = new Ext(std::move(config));

      /* warmup structure w/ 10% of records */
      size_t warmup = .1 * n;
      for (size_t j = 0; j < warmup; j++) {
        while (!extension->insert(data[j])) {
          usleep(1);
        }
      }

      extension->await_version();

      idx.store(warmup);

      size_t thrd_cnt = 8;
      std::thread thrds[thrd_cnt];

      for (size_t i=0; i<thrd_cnt; i++) {
        thrds[i] = std::thread(operation_thread, extension, &queries, &data);
      }

      for (size_t i=0; i<thrd_cnt; i++) {
        thrds[i].join();
      }

      fprintf(stderr, "%ld\n", total_res.load());
      delete extension;
    }
  }

  fflush(stderr);
}
