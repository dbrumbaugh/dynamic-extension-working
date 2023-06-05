/*
 * include/shard/MemISAM.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "framework/MutableBuffer.h"
#include "util/bf_config.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "util/timer.h"

namespace de {

thread_local size_t mrun_cancelations = 0;

template <RecordInterface R>
struct irs_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
    size_t sample_size;
    gsl_rng *rng;
};

template <RecordInterface R, bool Rejection>
class IRSQuery;

template <RecordInterface R>
struct IRSState {
    size_t lower_bound;
    size_t upper_bound;
};

template <RecordInterface R>
struct IRSBufferState {
    size_t cutoff;
    std::vector<Wrapped<R>> records;
};


template <RecordInterface R>
class MemISAM {
private:
    friend class InternalLevel;
    friend class IRSQuery<R, true>;
    friend class IRSQuery<R, false>;

typedef decltype(R::key) K;
typedef decltype(R::value) V;

constexpr static size_t inmem_isam_node_size = 256;
constexpr static size_t inmem_isam_fanout = inmem_isam_node_size / (sizeof(K) + sizeof(char*));

struct InMemISAMNode {
    K keys[inmem_isam_fanout];
    char* child[inmem_isam_fanout];
};

constexpr static size_t inmem_isam_leaf_fanout = inmem_isam_node_size / sizeof(R);
constexpr static size_t inmem_isam_node_keyskip = sizeof(K) * inmem_isam_fanout;

static_assert(sizeof(InMemISAMNode) == inmem_isam_node_size, "node size does not match");

public:
    MemISAM(MutableBuffer<R>* buffer)
    :m_reccnt(0), m_tombstone_cnt(0), m_isam_nodes(nullptr), m_deleted_cnt(0) {

        m_bf = new BloomFilter<K>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        TIMER_INIT();

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        TIMER_START();
        std::sort(base, stop, std::less<Wrapped<R>>());
        TIMER_STOP();
        auto sort_time = TIMER_RESULT();

        TIMER_START();
        while (base < stop) {
            if (!base->is_tombstone() && (base + 1 < stop)
                && base->rec == (base + 1)->rec  && (base + 1)->is_tombstone()) {
                base += 2;
                mrun_cancelations++;
                continue;
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            //Masking off the ts.
            base->header &= 1;
            m_data[m_reccnt++] = *base;
            if (m_bf && base->is_tombstone()) {
                ++m_tombstone_cnt;
                m_bf->insert(base->rec.key);
            }

            base++;
        }
        TIMER_STOP();
        auto copy_time = TIMER_RESULT();

        TIMER_START();
        if (m_reccnt > 0) {
            build_internal_levels();
        }
        TIMER_STOP();
        auto level_time = TIMER_RESULT();
    }

    MemISAM(MemISAM** runs, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_isam_nodes(nullptr) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(len);

        PriorityQueue<Wrapped<R>> pq(len);

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (runs[i]) {
                auto base = runs[i]->get_data();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count(), 0, runs[i]->get_record_count()});
                attemp_reccnt += runs[i]->get_record_count();
                tombstone_count += runs[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<K>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        size_t alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    if (cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        m_bf->insert(cursor.ptr->rec.key);
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            build_internal_levels();
        }
    }

    ~MemISAM() {
        if (m_data) free(m_data);
        if (m_isam_nodes) free(m_isam_nodes);
        if (m_bf) delete m_bf;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && !m_bf->lookup(rec.key)) {
            return nullptr;
        }

        size_t idx = get_lower_bound(rec.key);
        if (idx >= m_reccnt) {
            return nullptr;
        }

        while (idx < m_reccnt && m_data[idx].rec < rec) ++idx;

        if (m_data[idx].rec == rec) {
            return m_data + idx;
        }

        return nullptr;
    }

    Wrapped<R>* get_data() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const Wrapped<R>* get_record_at(size_t idx) const {
        return (idx < m_reccnt) ? m_data + idx : nullptr;
    }

    size_t get_memory_usage() {
        return m_reccnt * sizeof(R) + m_internal_node_cnt * inmem_isam_node_size;
    }

private:
    size_t get_lower_bound(const K& key) const {
        const InMemISAMNode* now = m_root;
        while (!is_leaf(reinterpret_cast<const char*>(now))) {
            const InMemISAMNode* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (now->child[i + 1] == nullptr || key <= now->keys[i]) {
                    next = reinterpret_cast<InMemISAMNode*>(now->child[i]);
                    break;
                }
            }

            now = next ? next : reinterpret_cast<const InMemISAMNode*>(now->child[inmem_isam_fanout - 1]);
        }

        const Wrapped<R>* pos = reinterpret_cast<const Wrapped<R>*>(now);
        while (pos < m_data + m_reccnt && pos->rec.key < key) pos++;

        return pos - m_data;
    }

    size_t get_upper_bound(const K& key) const {
        const InMemISAMNode* now = m_root;
        while (!is_leaf(reinterpret_cast<const char*>(now))) {
            const InMemISAMNode* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (now->child[i + 1] == nullptr || key < now->keys[i]) {
                    next = reinterpret_cast<InMemISAMNode*>(now->child[i]);
                    break;
                }
            }

            now = next ? next : reinterpret_cast<const InMemISAMNode*>(now->child[inmem_isam_fanout - 1]);
        }

        const Wrapped<R>* pos = reinterpret_cast<const Wrapped<R>*>(now);
        while (pos < m_data + m_reccnt && pos->rec.key <= key) pos++;

        return pos - m_data;
    }

    void build_internal_levels() {
        size_t n_leaf_nodes = m_reccnt / inmem_isam_leaf_fanout + (m_reccnt % inmem_isam_leaf_fanout != 0);
        size_t level_node_cnt = n_leaf_nodes;
        size_t node_cnt = 0;
        do {
            level_node_cnt = level_node_cnt / inmem_isam_fanout + (level_node_cnt % inmem_isam_fanout != 0);
            node_cnt += level_node_cnt;
        } while (level_node_cnt > 1);

        size_t alloc_size = (node_cnt * inmem_isam_node_size) + (CACHELINE_SIZE - (node_cnt * inmem_isam_node_size) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);

        m_isam_nodes = (InMemISAMNode*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);
        m_internal_node_cnt = node_cnt;
        memset(m_isam_nodes, 0, node_cnt * inmem_isam_node_size);

        InMemISAMNode* current_node = m_isam_nodes;

        const Wrapped<R>* leaf_base = m_data;
        const Wrapped<R>* leaf_stop = m_data + m_reccnt;
        while (leaf_base < leaf_stop) {
            size_t fanout = 0;
            for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                auto rec_ptr = leaf_base + inmem_isam_leaf_fanout * i;
                if (rec_ptr >= leaf_stop) break;
                const Wrapped<R>* sep_key = std::min(rec_ptr + inmem_isam_leaf_fanout - 1, leaf_stop - 1);
                current_node->keys[i] = sep_key->rec.key;
                current_node->child[i] = (char*)rec_ptr;
                ++fanout;
            }
            current_node++;
            leaf_base += fanout * inmem_isam_leaf_fanout;
        }

        auto level_start = m_isam_nodes;
        auto level_stop = current_node;
        auto current_level_node_cnt = level_stop - level_start;
        while (current_level_node_cnt > 1) {
            auto now = level_start;
            while (now < level_stop) {
                size_t child_cnt = 0;
                for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                    auto node_ptr = now + i;
                    ++child_cnt;
                    if (node_ptr >= level_stop) break;
                    current_node->keys[i] = node_ptr->keys[inmem_isam_fanout - 1];
                    current_node->child[i] = (char*)node_ptr;
                }
                now += child_cnt;
                current_node++;
            }
            level_start = level_stop;
            level_stop = current_node;
            current_level_node_cnt = level_stop - level_start;
        }
        
        assert(current_level_node_cnt == 1);
        m_root = level_start;
    }

    bool is_leaf(const char* ptr) const {
        return ptr >= (const char*)m_data && ptr < (const char*)(m_data + m_reccnt);
    }

    // Members: sorted data, internal ISAM levels, reccnt;
    Wrapped<R>* m_data;
    BloomFilter<K> *m_bf;
    InMemISAMNode* m_isam_nodes;
    InMemISAMNode* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_internal_node_cnt;
    size_t m_deleted_cnt;
};

template <RecordInterface R, bool Rejection=true>
class IRSQuery {
public:

    static void *get_query_state(MemISAM<R> *isam, void *parms) {
        auto res = new IRSState<R>();
        decltype(R::key) lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        decltype(R::key) upper_key = ((irs_query_parms<R> *) parms)->upper_bound;

        res->lower_bound = isam->get_lower_bound(lower_key);
        res->upper_bound = isam->get_upper_bound(upper_key);

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new IRSBufferState<R>();

        res->cutoff = buffer->get_record_count();

        if constexpr (Rejection) {
            return res;
        }

        auto lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        auto upper_key = ((irs_query_parms<R> *) parms)->upper_bound;

        for (size_t i=0; i<res->cutoff; i++) {
            if (((buffer->get_data() + i)->rec.key >= lower_key) && ((buffer->get_data() + i)->rec.key <= upper_key)) { 
                res->records.emplace_back(*(buffer->get_data() + i));
            }
        }

        return res;
    }

    static std::vector<Wrapped<R>> query(MemISAM<R> *isam, void *q_state, void *parms) { 
        auto sample_sz = ((irs_query_parms<R> *) parms)->sample_size;
        auto lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        auto upper_key = ((irs_query_parms<R> *) parms)->upper_bound;
        auto rng = ((irs_query_parms<R> *) parms)->rng;

        auto state = (IRSState<R> *) q_state;

        std::vector<Wrapped<R>> result_set;

        if (sample_sz == 0) {
            return result_set;
        }

        size_t attempts = 0;
        size_t range_length = state->upper_bound - state->lower_bound;
        do {
            attempts++;
            size_t idx = gsl_rng_uniform_int(rng, range_length);
            result_set.emplace_back(*isam->get_record_at(state->lower_bound + idx));
        } while (attempts < sample_sz);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (IRSBufferState<R> *) state;
        auto p = (irs_query_parms<R> *) parms;

        std::vector<Wrapped<R>> result;
        result.reserve(p->sample_size);

        if constexpr (Rejection) {
            for (size_t i=0; i<p->sample_size; i++) {
                auto idx = gsl_rng_uniform_int(p->rng, st->cutoff);
                auto rec = buffer->get_data() + idx;

                if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                    result.emplace_back(*rec);
                }
            }

            return result;
        }

        for (size_t i=0; i<p->sample_size; i++) {
            auto idx = gsl_rng_uniform_int(p->rng, st->records.size());
            result.emplace_back(st->records[idx]);
        }

        return result;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results) {
        std::vector<R> output;

        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                output.emplace_back(results[i][j]);
            }
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (IRSState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (IRSBufferState<R> *) state;
        delete s;
    }
};

}