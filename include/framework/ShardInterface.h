/*
 * include/framework/ShardInterface.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <concepts>

#include "util/types.h"
#include "framework/RecordInterface.h"

namespace de {

//template <template<typename> typename S, typename R>
template <typename S>
concept ShardInterface = requires(S s, void *p, bool b) {
    //{s.point_lookup(r, b) } -> std::same_as<R*>;
    {s.get_record_count()} -> std::convertible_to<size_t>;
    {s.get_memory_usage()} -> std::convertible_to<size_t>;
};

}
