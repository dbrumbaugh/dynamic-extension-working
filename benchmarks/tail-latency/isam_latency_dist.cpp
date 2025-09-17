/*
 *
 */

#include "framework/scheduling/FIFOScheduler.h"
#include "framework/scheduling/SerialScheduler.h"
#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"
#include "framework/util/Configuration.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::ISAMTree<Rec>Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE,
                            de::SerialScheduler>
    Conf;

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
    std::vector<size_t> buffers = {1000, 8000, 16000};
    std::vector<size_t> sfs = {4};
    
    for (size_t l=0; l<policies.size(); l++) {
    for (size_t j=0; j<buffers.size(); j++) {
    for (size_t k=0; k<sfs.size(); k++) {
        auto policy = get_policy<Shard, Q>(sfs[k], buffers[j], policies[l]);
        auto config = Conf(std::move(policy));

        config.recon_enable_maint_on_flush = true;
        config.recon_maint_disabled = false;
        
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

        for (size_t i=warmup; i<data.size(); i++) {
            TIMER_START();
            while (!extension->insert(data[i])) {
                usleep(1);
            }
            TIMER_STOP();

            fprintf(stdout, "I\t%ld\t%ld\t%d\t%ld\n", sfs[k], buffers[j], policies[l], TIMER_RESULT());
        }

        extension->await_version();
        
        /* repeat the queries a bunch of times */
        for (size_t l=0; l<10; l++) {
        for (size_t i=0; i<queries.size(); i++) {
            TIMER_START();
            auto q = queries[i];
            auto res = extension->query(std::move(q));
            res.get();
            TIMER_STOP();

            fprintf(stdout, "Q\t%ld\t%ld\t%d\t%ld\n", sfs[k], buffers[j], policies[l], TIMER_RESULT());
        }
        }


        QP p = {0, 10000};
        auto res =extension->query(std::move(p));

        fprintf(stderr, "%ld\n", res.get());
        delete extension;
    }}}


    fflush(stderr);
}

