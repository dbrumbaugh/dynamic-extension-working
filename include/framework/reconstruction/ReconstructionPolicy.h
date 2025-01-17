/*
 * include/framework/reconstruction/ReconstructionPolicy.h
 *
 * Reconstruction class interface, used to implement custom reconstruction
 * policies.
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "util/types.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/scheduling/Epoch.h"

namespace de {
template<ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class ReconstructionPolicy {
  typedef ExtensionStructure<ShardType, QueryType> StructureType;

public:
  ReconstructionPolicy() {}
  virtual ReconstructionVector get_reconstruction_tasks(const Version<ShardType, QueryType> *version, 
                                                        size_t incoming_reccnt) const = 0;
  virtual ReconstructionVector get_flush_tasks(const Version<ShardType, QueryType> *version) const = 0;
  };
}
