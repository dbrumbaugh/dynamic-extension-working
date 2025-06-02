/*
 *
 */

#include "framework/scheduling/SerialScheduler.h"
#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/DynamicExtension.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/util/Configuration.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"

#include "framework/reconstruction/FixedShardCountPolicy.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::ISAMTree<Rec> Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Conf;

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

    std::vector<size_t> shard_counts = {4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 4096*2};
    size_t buffer_size = 8000;

    for (size_t i=0; i<shard_counts.size(); i++) {
        auto policy = get_policy<Shard, Q>(shard_counts[i], buffer_size, 4, n);
        auto config = Conf(std::move(policy));
        config.recon_enable_maint_on_flush = false;
        config.recon_maint_disabled = true;
        config.buffer_flush_trigger = 4000;
        config.maximum_threads = 8;

        auto extension = new Ext(std::move(config));

        /* warmup structure w/ 10% of records */
        size_t warmup = .3 * n;
        for (size_t i=0; i<warmup; i++) {
            while (!extension->insert(data[i])) {
                usleep(1);
            }
        }

        extension->await_version();

        TIMER_INIT();

        TIMER_START();
        for (size_t i=warmup; i<data.size(); i++) {
            while (!extension->insert(data[i])) {
                usleep(1);
            }
        }
        TIMER_STOP();

        auto insert_tput = (size_t) ((double) (n - warmup) / (double) TIMER_RESULT() *1.0e9);

        extension->await_version();
        
        /* repeat the queries a bunch of times */
        TIMER_START();
        for (size_t l=0; l<10; l++) {
        for (size_t i=0; i<queries.size(); i++) {
            auto q = queries[i];
            auto res = extension->query(std::move(q));
            res.get();
        }
        }
        TIMER_STOP();

        auto query_lat = TIMER_RESULT() / 10*queries.size();

        fprintf(stdout, "%ld\t%ld\t%ld\t%ld\n", shard_counts[i], extension->get_shard_count(), insert_tput, query_lat);

        delete extension;
    }

    fflush(stderr);
}

