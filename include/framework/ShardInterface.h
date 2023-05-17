/*
 * include/shard/ShardInterface.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <concepts>

#include "util/types.h"

template <typename S>
concept ShardInterface = requires(S s, void *p) {
    //s.point_lookup();
    //s.tombstone_lookup();
    //s.delete_record();

    {s.get_query_state(p)} -> std::convertible_to<void*>;
    {s.delete_query_state(p)};

    {s.get_record_count()} -> std::convertible_to<size_t>;
    {s.get_tombstone_count()} -> std::convertible_to<size_t>;
    {s.get_memory_usage()} -> std::convertible_to<size_t>;
};
