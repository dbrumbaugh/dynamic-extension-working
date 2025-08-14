/*
 *
 */

#define ENABLE_TIMER
#define TS_TEST

#include "framework/scheduling/FIFOScheduler.h"
#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"
#include "framework/util/Configuration.h"

#include <gsl/gsl_rng.h>


typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::ISAMTree<Rec>Shard;
typedef de::rc::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::DeletePolicy::TOMBSTONE, de::FIFOScheduler> Ext;
typedef Q::Parameters QP;
typedef de::DEConfiguration<Shard, Q, de::DeletePolicy::TOMBSTONE,
                            de::FIFOScheduler>
    Conf;

void usage(char *progname) {
    fprintf(stderr, "%s reccnt datafile\n", progname);
}

int main(int argc, char **argv) {

    if (argc < 3) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);

    
    auto data = read_sosd_file<Rec>(d_fname, n);

    std::vector<int> policies = {6};
    std::vector<size_t> buffers = {12000}; 
    std::vector<size_t> sfs = {8};

    for (size_t l=0; l<policies.size(); l++) {
    for (size_t j=0; j<buffers.size(); j++) {
    for (size_t k=0; k<sfs.size(); k++) {
        auto policy = get_policy<Shard, Q>(sfs[k], buffers[j], policies[l]);
        auto config = Conf(std::move(policy));
        config.recon_enable_maint_on_flush = true;
        config.recon_maint_disabled = false;
        // config.buffer_flush_trigger = 4000;
        config.maximum_threads = 6;
        auto extension = new Ext(std::move(config));

        /* warmup structure w/ 10% of records */
        size_t warmup = .1 * n;
        for (size_t i=0; i<warmup; i++) {
            while (!extension->insert(data[i])) {
                usleep(1);
            }
        }

        extension->await_version();

        for (size_t i=warmup; i<data.size(); i++) {
            while (!extension->insert(data[i])) {
                usleep(1);
            }
        }

        extension->await_version();
        
        extension->print_scheduler_statistics();
        delete extension;
       
    }}}


    fflush(stderr);
}

