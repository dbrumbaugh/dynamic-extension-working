/*
 * include/query/rangequery.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include "framework/interface/Record.h"
#include "framework/interface/Shard.h"
#include "framework/structure/MutableBuffer.h"

namespace de { namespace wss {

template <WeightedRecordInterface R>
struct Parms {
    size_t sample_size;
    gsl_rng *rng;
};

template <WeightedRecordInterface R>
struct State {
    decltype(R::weight) total_weight;
    size_t sample_size;

    State() {
        total_weight = 0;
    }
};

template <RecordInterface R>
struct BufferState {
    size_t cutoff;
    size_t sample_size;
    psudb::Alias *alias;
    decltype(R::weight) max_weight;
    decltype(R::weight) total_weight;

    ~BufferState() {
        delete alias;
    }
};

template <ShardInterface S, RecordInterface R, bool Rejection=true>
class Query {
public:
    constexpr static bool EARLY_ABORT=false;
    constexpr static bool SKIP_DELETE_FILTER=false;

    static void *get_query_state(S *shard, void *parms) {
        auto res = new State<R>();
        res->total_weight = shard->get_total_weight();
        res->sample_size = 0;

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        BufferState<R> *state = new BufferState<R>();
        auto parameters = (Parms<R>*) parms;
        if constexpr (Rejection) {
            state->cutoff = buffer->get_record_count() - 1;
            state->max_weight = buffer->get_max_weight();
            state->total_weight = buffer->get_total_weight();
            return state;
        }

        std::vector<double> weights;

        state->cutoff = buffer->get_record_count() - 1;
        double total_weight = 0.0;

        for (size_t i = 0; i <= state->cutoff; i++) {
            auto rec = buffer->get_data() + i;
            weights.push_back(rec->rec.weight);
            total_weight += rec->rec.weight;
        }

        for (size_t i = 0; i < weights.size(); i++) {
            weights[i] = weights[i] / total_weight;
        }

        state->alias = new psudb::Alias(weights);
        state->total_weight = total_weight;

        return state;
    }

    static void process_query_states(void *query_parms, std::vector<void*> &shard_states, std::vector<void*> &buffer_states) {
        auto p = (Parms<R> *) query_parms;
        auto bs = (BufferState<R> *) buffer_states[0];

        std::vector<size_t> shard_sample_sizes(shard_states.size()+1, 0);
        size_t buffer_sz = 0;

        std::vector<decltype(R::weight)> weights;
        weights.push_back(bs->total_weight);

        decltype(R::weight) total_weight = 0;
        for (auto &s : shard_states) {
            auto state = (State<R> *) s;
            total_weight += state->total_weight;
            weights.push_back(state->total_weight);
        }

        std::vector<double> normalized_weights;
        for (auto w : weights) {
            normalized_weights.push_back((double) w / (double) total_weight);
        }

        auto shard_alias = psudb::Alias(normalized_weights);
        for (size_t i=0; i<p->sample_size; i++) {
            auto idx = shard_alias.get(p->rng);            
            if (idx == 0) {
                buffer_sz++;
            } else {
                shard_sample_sizes[idx - 1]++;
            }
        }


        bs->sample_size = buffer_sz;
        for (size_t i=0; i<shard_states.size(); i++) {
            auto state = (State<R> *) shard_states[i];
            state->sample_size = shard_sample_sizes[i+1];
        }
    }

    static std::vector<Wrapped<R>> query(S *shard, void *q_state, void *parms) {
        auto rng = ((Parms<R> *) parms)->rng;

        auto state = (State<R> *) q_state;
        auto sample_size = state->sample_size;

        std::vector<Wrapped<R>> result_set;

        if (sample_size == 0) {
            return result_set;
        }
        size_t attempts = 0;
        do {
            attempts++;
            size_t idx = shard->m_alias->get(rng);
            result_set.emplace_back(*shard->get_record_at(idx));
        } while (attempts < sample_size);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (BufferState<R> *) state;
        auto p = (Parms<R> *) parms;

        std::vector<Wrapped<R>> result;
        result.reserve(st->sample_size);

        if constexpr (Rejection) {
            for (size_t i=0; i<st->sample_size; i++) {
                auto idx = gsl_rng_uniform_int(p->rng, st->cutoff);
                auto rec = buffer->get_data() + idx;

                auto test = gsl_rng_uniform(p->rng) * st->max_weight;

                if (test <= rec->rec.weight) {
                    result.emplace_back(*rec);
                }
            }
            return result;
        }

        for (size_t i=0; i<st->sample_size; i++) {
            auto idx = st->alias->get(p->rng);
            result.emplace_back(*(buffer->get_data() + idx));
        }

        return result;
    }

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms) {
        std::vector<R> output;

        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                output.emplace_back(results[i][j].rec);
            }
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (State<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (BufferState<R> *) state;
        delete s;
    }
};

}}
