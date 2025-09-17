/*
 *
 */

#define ENABLE_TIMER

#include "shard/ISAMTree.h"
#include "query/rangequery.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "benchmark_types.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"
#include "standard_benchmarks.h"
#include "psu-ds/BTree.h"

typedef btree_record<int64_t, int64_t> Rec;

typedef de::ISAMTree<Rec> Shard;
typedef de::irs::Query<Shard> Q;
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

    auto btree = BenchBTree();
    
    auto data = read_sosd_file<Rec>(d_fname, n);

    /* read in the range queries and add sample size and rng for sampling */
    auto queries = read_range_queries<QP>(q_fname, .0001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .3 * n;
    for (size_t i=0; i<warmup; i++) {
        btree.insert(data[i]);
    }

    TIMER_INIT();
    /* insertion benchmark */
    TIMER_START();
    for (size_t i=warmup; i<data.size(); i++) {
        btree.insert(data[i]);
    }
    TIMER_STOP();

    size_t insert_tput =
        ((double)(n - warmup) / (double)TIMER_RESULT()) * 1e9;

    fprintf(stdout, "%ld\n", insert_tput);

    // /* run queries */
    // TIMER_START();
    // size_t total = 0;
    // for (size_t j=0; j<10; j++) {
    //     for (size_t i=0; i<queries.size(); i++) {
    //         total += btree.range_count(queries[i].lower_bound, queries[i].upper_bound);
    //     }
    // }
    // TIMER_STOP();

    // fprintf(stderr, "%ld\n", total);
}

