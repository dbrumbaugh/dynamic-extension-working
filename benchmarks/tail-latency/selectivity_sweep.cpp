/*
 *
 */

#include "framework/scheduling/SerialScheduler.h"
#include "framework/util/Configuration.h"
#include "util/types.h"
#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/DynamicExtension.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
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
    fprintf(stderr, "%s reccnt\n", progname);
}

int main(int argc, char **argv) {

    if (argc < 2) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = "unif";
    
    auto data = read_sosd_file<Rec>(d_fname, n);

    std::vector<std::vector<QP>> query_sets;
    std::vector<double> selectivities = {0, 0.00000001, 0.0000001, 0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1};

    for (auto sel: selectivities) {
        query_sets.push_back(generate_uniform_range_queries<QP>(100, n, sel));
    }

    std::vector<size_t> sfs = {2, 4, 8}; //, 4, 8, 16, 32, 64, 128, 256, 512, 1024}; 
    size_t buffer_size = 8000;
    std::vector<size_t> policies = {0, 1};

    for (auto pol: policies) {
    for (size_t i=0; i<sfs.size(); i++) {
        auto policy = get_policy<Shard, Q>(sfs[i], buffer_size, pol, n);
        auto config = Conf(std::move(policy));
        config.recon_enable_maint_on_flush = false;
        config.recon_maint_disabled = true;
        config.buffer_flush_trigger = 4000;
        config.maximum_threads = 8;
        
        auto extension = new Ext(std::move(config));

        /* warmup structure w/ 10% of records */
        size_t warmup = .1 * n;
        for (size_t j=0; j<warmup; j++) {
            while (!extension->insert(data[j])) {
                usleep(1);
            }
        }

        extension->await_version();

        // fprintf(stderr, "\n[I] Running Insertion Benchmark\n");

        TIMER_INIT();

        TIMER_START();
        for (size_t j=warmup; j<data.size(); j++) {
            while (!extension->insert(data[j])) {
                usleep(1);
                fprintf(stderr, "insert blocked %ld\r", j);
            }
        }
        TIMER_STOP();
        auto total_insert_lat = TIMER_RESULT();

        // extension->print_structure();
        // fflush(stdout);

        // fprintf(stderr, "\n[I] Finished running insertion benchmark\n");
        extension->await_version();

        // fprintf(stderr, "[I] Running query benchmark\n");
        
        /* repeat the queries a bunch of times */

        auto insert_throughput =  (size_t) ((double) (n - warmup) / (double) total_insert_lat *1.0e9);
        fprintf(stdout, "%ld\t%ld\t%ld\t%ld\t", pol, sfs[i], n, insert_throughput);

        size_t total = 0;
        for (size_t l=0; l<query_sets.size(); l++) {
            TIMER_START();
            for (size_t f=0; f<query_sets[l].size()*10; f++) {
                auto q = query_sets[l][f%10];
                auto res = extension->query(std::move(q));
                total += res.get();
            }
            TIMER_STOP();
            auto query_latency = (TIMER_RESULT()) / (10*query_sets[l].size());
            fprintf(stdout, "%lf\t%ld\t", selectivities[l], query_latency);
        }

        fprintf(stdout, "\n");
        fprintf(stderr, "%ld\n", total);
        fflush(stdout);

        // extension->print_structure();
        delete extension;
    }
    }

    fflush(stderr);
}

