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
  DEConfiguration(std::unique_ptr<ReconstructionPolicy<ShardType, QueryType>> recon_policy) 
   : m_recon_policy(recon_policy) {}

   public:
    std::unique_ptr<ReconstructionPolicy<ShardType, QueryType>> m_recon_policy;
};

} // namespace de
