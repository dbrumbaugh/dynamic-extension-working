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
#include "shard/TrieSpline.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"

#include "framework/reconstruction/FixedShardCountPolicy.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::TrieSpline<Rec> Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE, de::FIFOScheduler> Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE, de::FIFOScheduler> Conf;

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
    std::vector<size_t> policies = {0,};

    for (auto pol: policies) {
    for (size_t i=0; i<sfs.size(); i++) {
        auto policy = get_policy<Shard, Q>(sfs[i], buffer_size, pol, n);
        auto config = Conf(std::move(policy));
        config.recon_enable_maint_on_flush = false;
        config.recon_maint_disabled = true;
        config.buffer_flush_trigger = 4000;
        
        auto extension = new Ext(std::move(config));

        /* warmup structure w/ 10% of records */
        size_t warmup = .1 * n;
        for (size_t j=0; j<warmup; j++) {
            while (!extension->insert(data[j])) {
                usleep(1);
            }
        }

        extension->await_version();

        TIMER_INIT();

        for (size_t j=warmup; j<data.size(); j++) {
            TIMER_START();
            while (!extension->insert(data[j])) {
                usleep(1);
            }
            TIMER_STOP();
            fprintf(stdout, "I\t%ld\n", TIMER_RESULT());
        }

        extension->await_version();
        
        size_t total = 0;
        /* repeat the queries a bunch of times */
        for (size_t l=0; l<10; l++) {
            for (size_t j=0; j<queries.size(); j++) {
                TIMER_START();
                auto q = queries[j];
                auto res = extension->query(std::move(q));
                total += res.get();
                TIMER_STOP();
                fprintf(stdout, "Q\t%ld\n", TIMER_RESULT());
            }
        }

        fprintf(stderr, "%ld\n", total);

        extension->print_structure();
        delete extension;
    }
    }

    fflush(stderr);
}

