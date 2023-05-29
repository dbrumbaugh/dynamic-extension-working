/*
    {s.get_tombstone_count()} -> std::convertible_to<size_t>;
 * include/shard/WIRS.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once


#include <vector>
#include <cassert>
#include <queue>
#include <memory>
#include <concepts>

#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/Alias.h"
#include "ds/BloomFilter.h"
#include "util/bf_config.h"
#include "framework/MutableBuffer.h"
#include "framework/RecordInterface.h"

namespace de {

thread_local size_t wirs_cancelations = 0;

template <WeightedRecordInterface R>
struct wirs_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
};

class InternalLevel;

template <WeightedRecordInterface R>
class WIRSQuery;

template <WeightedRecordInterface R>
struct wirs_node {
    struct wirs_node<R> *left, *right;
    decltype(R::key) low, high;
    decltype(R::weight) weight;
    Alias* alias;
};

template <WeightedRecordInterface R>
struct WIRSState {
    decltype(R::weight) tot_weight;
    std::vector<wirs_node<R>*> nodes;
    Alias* top_level_alias;

    ~WIRSState() {
        if (top_level_alias) delete top_level_alias;
    }
};

template <WeightedRecordInterface R>
class WIRS {
    friend class InternalLevel;
private:

    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:

    friend class WIRSQuery<R>;

    WIRS(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_root(nullptr) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(R)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(R)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (R*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        m_bf = new BloomFilter<K>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->sorted_output();
        auto stop = base + buffer->get_record_count();

        while (base < stop) {
            if (!(base->is_tombstone()) && (base + 1) < stop) {
                if (*base == *(base + 1) && (base + 1)->is_tombstone()) {
                    base += 2;
                    wirs_cancelations++;
                    continue;
                }
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            base->header &= 1;
            m_data[m_reccnt++] = *base;
            m_total_weight+= base->weight;

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->key);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    WIRS(WIRS** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_root(nullptr) {
        std::vector<Cursor<R>> cursors;
        cursors.reserve(len);

        PriorityQueue<R> pq(len);

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (shards[i]) {
                auto base = shards[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                tombstone_count += shards[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor<R>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<K>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        size_t alloc_size = (attemp_reccnt * sizeof(R)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(R)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (R*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<R>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                *now.data == *next.data && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor<R>(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor<R>(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    m_total_weight += cursor.ptr->weight;
                    if (m_bf && cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        if (m_bf) m_bf->insert(cursor.ptr->key);
                    }
                }
                pq.pop();
                
                if (advance_cursor<R>(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
   }

    ~WIRS() {
        if (m_data) free(m_data);
        for (size_t i=0; i<m_alias.size(); i++) {
            if (m_alias[i]) delete m_alias[i];
        }

        if (m_bf) delete m_bf;

        free_tree(m_root);
    }

    R *point_lookup(R &rec, bool filter=false) {
        if (filter && !m_bf.lookup(rec.key)) {
            return nullptr;
        }

        size_t idx = get_lower_bound(rec.key);
        if (idx >= m_reccnt) {
            return nullptr;
        }

        while (idx < m_reccnt && m_data[idx] < rec) ++idx;

        if (m_data[idx] == rec) {
            return m_data + idx;
        }

        return nullptr;
    }

    R* sorted_output() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const R* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }


    size_t get_memory_usage() {
        return 0;
    }

private:

    size_t get_lower_bound(const K& key) const {
        size_t min = 0;
        size_t max = m_reccnt - 1;

        const char * record_key;
        while (min < max) {
            size_t mid = (min + max) / 2;

            if (key > m_data[mid].key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return min;
    }

    bool covered_by(struct wirs_node<R>* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[low_index].key && m_data[high_index].key < upper_key;
    }

    bool intersects(struct wirs_node<R>* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[high_index].key && m_data[low_index].key < upper_key;
    }

    void build_wirs_structure() {
        m_group_size = std::ceil(std::log(m_reccnt));
        size_t n_groups = std::ceil((double) m_reccnt / (double) m_group_size);
        
        // Fat point construction + low level alias....
        double sum_weight = 0.0;
        std::vector<W> weights;
        std::vector<double> group_norm_weight;
        size_t i = 0;
        size_t group_no = 0;
        while (i < m_reccnt) {
            double group_weight = 0.0;
            group_norm_weight.clear();
            for (size_t k = 0; k < m_group_size && i < m_reccnt; ++k, ++i) {
                auto w = m_data[i].weight;
                group_norm_weight.emplace_back(w);
                group_weight += w;
                sum_weight += w;
            }

            for (auto& w: group_norm_weight)
                if (group_weight) w /= group_weight;
                else w = 1.0 / group_norm_weight.size();
            m_alias.emplace_back(new Alias(group_norm_weight));

            
            weights.emplace_back(group_weight);
        }

        assert(weights.size() == n_groups);

        m_root = construct_wirs_node(weights, 0, n_groups-1);
    }

    void free_tree(struct wirs_node<R>* node) {
        if (node) {
            delete node->alias;
            free_tree(node->left);
            free_tree(node->right);
            delete node;
        }
    }

    R* m_data;
    std::vector<Alias *> m_alias;
    wirs_node<R>* m_root;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    BloomFilter<K> m_bf;
};


template <WeightedRecordInterface R>
class WIRSQuery {
public:
    static void *get_query_state(wirs_query_parms<R> *parameters, WIRS<R> *wirs) {
        auto res = new WIRSState<R>();
        decltype(R::key) lower_key = ((wirs_query_parms<R> *) parameters)->lower_bound;
        decltype(R::key) upper_key = ((wirs_query_parms<R> *) parameters)->upper_bound;

        // Simulate a stack to unfold recursion.        
        double tot_weight = 0.0;
        struct wirs_node<R>* st[64] = {0};
        st[0] = wirs->m_root;
        size_t top = 1;
        while(top > 0) {
            auto now = st[--top];
            if (covered_by(now, lower_key, upper_key) ||
                (now->left == nullptr && now->right == nullptr && intersects(now, lower_key, upper_key))) {
                res->nodes.emplace_back(now);
                tot_weight += now->weight;
            } else {
                if (now->left && intersects(now->left, lower_key, upper_key)) st[top++] = now->left;
                if (now->right && intersects(now->right, lower_key, upper_key)) st[top++] = now->right;
            }
        }
        
        std::vector<double> weights;
        for (const auto& node: res->nodes) {
            weights.emplace_back(node->weight / tot_weight);
        }
        res->tot_weight = tot_weight;
        res->top_level_alias = new Alias(weights);

        return res;
    }

    static std::vector<R> *query(wirs_query_parms<R> *parameters, WIRSState<R> *state, WIRS<R> *wirs) {
        auto sample_sz = parameters->sample_size;
        auto lower_key = parameters->lower_bound;
        auto upper_key = parameters->upper_bound;
        auto rng = parameters->rng;

        std::vector<R> *result_set = new std::vector<R>();

        if (sample_sz == 0) {
            return 0;
        }
        // k -> sampling: three levels. 1. select a node -> select a fat point -> select a record.
        size_t cnt = 0;
        size_t attempts = 0;
        do {
            ++attempts;
            // first level....
            auto node = state->nodes[state->top_level_alias->get(rng)];
            // second level...
            auto fat_point = node->low + node->alias->get(rng);
            // third level...
            size_t rec_offset = fat_point * wirs->m_group_size + wirs->m_alias[fat_point]->get(rng);
            auto record = wirs->m_data + rec_offset;

            // bounds rejection
            if (lower_key > record->key || upper_key < record->key) {
                continue;
            } 

            result_set->emplace_back(*record);
            cnt++;
        } while (attempts < sample_sz);

        return result_set;
    }

    static std::vector<R> *merge(std::vector<std::vector<R>> *results) {
        std::vector<R> *output = new std::vector<R>();

        for (size_t i=0; i<results->size(); i++) {
            for (size_t j=0; j<(*results)[i]->size(); j++) {
                output->emplace_back(*((*results)[i])[j]);
            }
        }
        return output;
    }

    static void delete_query_state(wirs_query_parms<R> *parameters) {
        delete parameters;
    }


    //{q.get_buffer_query_state(p, p)};
    //{q.buffer_query(p, p)};

};

}
