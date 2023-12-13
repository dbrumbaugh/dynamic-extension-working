/*
 * include/framework/structure/MutableBuffer.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * FIXME: currently, the buffer itself is responsible for managing a
 * secondary buffer for storing sorted records used during buffer flushes. It
 * probably makes more sense to make the shard being flushed into responsible
 * for this instead. This would also facilitate simultaneous flushes of multiple
 * buffers more easily.
 *
 */
#pragma once

#include <cstdlib>
#include <atomic>
#include <condition_variable>
#include <cassert>
#include <numeric>
#include <algorithm>
#include <type_traits>

#include "psu-util/alignment.h"
#include "util/bf_config.h"
#include "psu-ds/BloomFilter.h"
#include "psu-ds/Alias.h"
#include "psu-util/timer.h"
#include "framework/interface/Record.h"

using psudb::CACHELINE_SIZE;

namespace de {

template <RecordInterface R>
class MutableBuffer {
public:
    MutableBuffer(size_t capacity, size_t max_tombstone_cap)
    : m_cap(capacity), m_tombstone_cap(capacity), m_reccnt(0)
    , m_tombstonecnt(0), m_weight(0), m_max_weight(0), m_tail(0) {
        m_data = (Wrapped<R>*) psudb::sf_aligned_alloc(CACHELINE_SIZE, capacity*sizeof(Wrapped<R>));
        m_sorted_data = (Wrapped<R>*) psudb::sf_aligned_alloc(CACHELINE_SIZE, capacity*sizeof(Wrapped<R>));
        m_tombstone_filter = nullptr;
        if (max_tombstone_cap > 0) {
            m_tombstone_filter = new psudb::BloomFilter<R>(BF_FPR, max_tombstone_cap, BF_HASH_FUNCS);
        }

        m_refcnt.store(0);
    }

    ~MutableBuffer() {
        assert(m_refcnt.load() == 0);

        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
        if (m_sorted_data) free(m_sorted_data);
    }

    template <typename R_ = R>
    int append(const R &rec, bool tombstone=false) {
        if (tombstone && m_tombstonecnt + 1 > m_tombstone_cap) return 0;

        int32_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        Wrapped<R> wrec;
        wrec.rec = rec;
        wrec.header = 0;
        if (tombstone) wrec.set_tombstone();

        m_data[pos] = wrec;
        m_data[pos].header |= (pos << 2);

        if (tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(rec);
        }

        if constexpr (WeightedRecordInterface<R_>) {
            m_weight.fetch_add(rec.weight);
            double old = m_max_weight.load();
            while (old < rec.weight) {
                m_max_weight.compare_exchange_strong(old, rec.weight);
                old = m_max_weight.load();
            }
        } else {
            m_weight.fetch_add(1);
        }

        m_reccnt.fetch_add(1);
        return 1;     
    }

    bool truncate() {
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        m_weight.store(0);
        m_max_weight.store(0);
        m_tail.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    size_t get_record_count() {
        return m_reccnt;
    }
    
    size_t get_capacity() {
        return m_cap;
    }

    bool is_full() {
        return m_reccnt == m_cap;
    }

    size_t get_tombstone_count() {
        return m_tombstonecnt.load();
    }

    bool delete_record(const R& rec) {
        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].rec == rec) {
                m_data[offset].set_delete();
                return true;
            }
            offset++;
        }

        return false;
    }

    bool check_tombstone(const R& rec) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(rec)) return false;

        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].rec == rec && m_data[offset].is_tombstone()) {
                return true;
            }
            offset++;;
        }
        return false;
    }

    size_t get_memory_usage() {
        return m_cap * sizeof(R);
    }

    size_t get_aux_memory_usage() {
        return m_tombstone_filter->get_memory_usage();
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

    double get_total_weight() {
        return m_weight.load();
    }

    Wrapped<R> *get_data() {
        return m_data;
    }

    double get_max_weight() {
        return m_max_weight;
    }

    /*
     * This operation assumes that no other threads have write access
     * to the buffer. This will be the case in normal operation, at
     * present, but may change (in which case this approach will need
     * to be adjusted). Other threads having read access is perfectly
     * acceptable, however.
     */
    bool start_flush() {
        memcpy(m_sorted_data, m_data, sizeof(Wrapped<R>) * m_reccnt.load());
        return true;
    }

    /*
     * Concurrency-related operations
     */
    bool take_reference() {
        m_refcnt.fetch_add(1);
        return true;
    }

    bool release_reference() {
        assert(m_refcnt > 0);
        m_refcnt.fetch_add(-1);
        return true;
    }

    size_t get_reference_count() {
        return m_refcnt.load();
    }

private:
    int64_t try_advance_tail() {
        int64_t new_tail = m_tail.fetch_add(1);

        if (new_tail < m_cap) {
            return new_tail;
        } 

        m_tail.fetch_add(-1);
        return -1;
    }

    size_t m_cap;
    size_t m_tombstone_cap;
    
    Wrapped<R>* m_data;
    Wrapped<R>* m_sorted_data;

    psudb::BloomFilter<R>* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<uint32_t> m_reccnt;
    alignas(64) std::atomic<int64_t> m_tail;
    alignas(64) std::atomic<double> m_weight;
    alignas(64) std::atomic<double> m_max_weight;

    alignas(64) std::atomic<size_t> m_refcnt;
};

}
