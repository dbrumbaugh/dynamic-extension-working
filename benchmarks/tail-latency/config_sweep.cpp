/*
 *
 */

#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/TrieSpline.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::TrieSpline<Rec> Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Ext;
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

    std::vector<int> policies = {0, 1};
    std::vector<size_t> buffers = {4000, 8000, 12000, 16000, 20000};
    std::vector<size_t> sfs = {2, 4, 6, 8, 12};

    for (size_t l=0; l<policies.size(); l++) {
    for (size_t j=0; j<buffers.size(); j++) {
    for (size_t k=0; k<sfs.size(); k++) {
        auto policy = get_policy<Shard, Q>(sfs[k], buffers[j], policies[l]);
        auto extension = new Ext(policy, 8000);

        /* warmup structure w/ 10% of records */
        size_t warmup = .1 * n;
        for (size_t i=0; i<warmup; i++) {
            while (!extension->insert(data[i])) {
                usleep(1);
            }
        }

        extension->await_next_epoch();

        TIMER_INIT();

        for (size_t i=warmup; i<data.size(); i++) {
            TIMER_START();
            while (!extension->insert(data[i])) {
                usleep(1);
            }
            TIMER_STOP();

            fprintf(stdout, "%ld\t%ld\t%d\t%ld\n", sfs[k], buffers[j], policies[l], TIMER_RESULT());
        }


        QP p = {0, 10000};
        auto res =extension->query(std::move(p));

        fprintf(stderr, "%ld\n", res.get());
        extension->await_next_epoch();
        delete extension;
    }}}

    /*
    std::vector<int64_t> query_latencies;
    query_latencies.reserve(queries.size());
    for (size_t i=warmup; i<data.size(); i++) {
        TIMER_START();
        auto q = queries[i];
        auto res = extension->query(std::move(q));
        res.get();
        TIMER_STOP();

        query_latencies.push_back(TIMER_RESULT());
    }

    printf("here\n");

    for (size_t i=0; i<insert_latencies.size(); i++) {
        fprintf(stdout, "I\t%ld\n", insert_latencies[i]);
    }

    for (size_t i=0; i<query_latencies.size(); i++) {
        fprintf(stdout, "Q\t%ld\n", query_latencies[i]);
    }
    */

    fflush(stderr);
}

