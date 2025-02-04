/*
 *
 */

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

    std::vector<size_t> sfs = {2, 4, 8}; //, 4, 8, 16, 32, 64, 128, 256, 512, 1024}; 
    size_t buffer_size = 8000;
    std::vector<size_t> policies = {0, 1};

    for (auto pol: policies) {
    for (size_t i=0; i<sfs.size(); i++) {
        auto policy = get_policy<Shard, Q>(sfs[i], buffer_size, pol);
        auto extension = new Ext(policy, buffer_size / 4, buffer_size);

        /* warmup structure w/ 10% of records */
        size_t warmup = .1 * n;
        for (size_t j=0; j<warmup; j++) {
            while (!extension->insert(data[j])) {
                usleep(1);
            }
        }

        extension->await_next_epoch();

        TIMER_INIT();

        TIMER_START();
        for (size_t j=warmup; j<data.size(); j++) {
            while (!extension->insert(data[j])) {
                usleep(1);
            }
        }
        TIMER_STOP();

        size_t insert_tput = (double) (n - warmup) / (double) (TIMER_RESULT()) * 1e9;

        extension->await_next_epoch();
        
        size_t total = 0;
        TIMER_START();
        /* repeat the queries a bunch of times */
        for (size_t l=0; l<10; l++) {
            for (size_t j=0; j<queries.size(); j++) {
                auto q = queries[j];
                auto res = extension->query(std::move(q));
                total += res.get();
            }
        }
        TIMER_STOP();

        size_t query_lat = (double) TIMER_RESULT() / (10*queries.size());

        fprintf(stdout, "S\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\n", pol, sfs[i], extension->get_height(), extension->get_shard_count(), total, insert_tput, query_lat);
        delete extension;
    }
    }

    fflush(stderr);
}

