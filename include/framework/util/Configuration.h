/*
 * include/framework/util/Configuration.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/reconstruction/ReconstructionPolicy.h"
#include "util/types.h"
#include "framework/interface/Scheduler.h"
#include <cstdlib>

namespace de {

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
DeletePolicy D, SchedulerInterface SchedType>
class DEConfiguration {
  public:
  DEConfiguration(std::unique_ptr<ReconstructionPolicy<ShardType, QueryType>> recon_policy) 
   : recon_policy(std::move(recon_policy)) {}

    std::unique_ptr<ReconstructionPolicy<ShardType, QueryType>> recon_policy;

    /* buffer parameters */
    size_t buffer_count = 1;
    size_t buffer_size = 8000;
    size_t buffer_flush_trigger = buffer_size / 4;

    /* reconstruction triggers */
    bool recon_enable_seek_trigger = false;
    bool recon_enable_maint_on_flush = false;
    bool recon_enable_delete_cmpct = false;
    bool recon_maint_disabled = true;

    size_t recon_l0_capacity = 0; /* 0 for unbounded */
    double maximum_delete_proportion = 1;

    /* resource management */
    size_t maximum_threads = 16;
    size_t minimum_recon_threads = 1; 
    size_t minimum_query_threads = 4;
    size_t maximum_memory_usage = 0; /* o for unbounded */

};

} // namespace de
