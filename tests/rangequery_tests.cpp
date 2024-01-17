/*
 * tests/rangequery_tests.cpp
 *
 * Unit tests for Range Queries across several different
 * shards
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

#include "shard/ISAMTree.h"
#include "query/rangequery.h"
#include "include/testing.h"

#include <check.h>

using namespace de;

typedef ISAMTree<Rec> Shard;

START_TEST(t_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);
    auto shard = Shard(buffer->get_buffer_view());

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_query_state(&shard, &parms);
    auto result = rq::Query<Shard, Rec>::query(&shard, state, &parms);
    rq::Query<Shard, Rec>::delete_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_buffer_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_buffer_query_state(buffer->get_buffer_view(), &parms);
    auto result = rq::Query<Shard, Rec>::buffer_query(state, &parms);
    rq::Query<Shard, Rec>::delete_buffer_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_range_query_merge)
{    
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    rq::Parms<Rec> parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto state1 = rq::Query<Shard, Rec>::get_query_state(&shard1, &parms);
    auto state2 = rq::Query<Shard, Rec>::get_query_state(&shard2, &parms);

    std::vector<std::vector<de::Wrapped<Rec>>> results(2);
    results[0] = rq::Query<Shard, Rec>::query(&shard1, state1, &parms);
    results[1] = rq::Query<Shard, Rec>::query(&shard2, state2, &parms);

    rq::Query<Shard, Rec>::delete_query_state(state1);
    rq::Query<Shard, Rec>::delete_query_state(state2);

    ck_assert_int_eq(results[0].size() + results[1].size(), result_size);

    std::vector<std::vector<Wrapped<Rec>>> proc_results;

    for (size_t j=0; j<results.size(); j++) {
        proc_results.emplace_back(std::vector<Wrapped<Rec>>());
        for (size_t i=0; i<results[j].size(); i++) {
            proc_results[j].emplace_back(results[j][i]);
        }
    }

    auto result = rq::Query<Shard, Rec>::merge(proc_results, nullptr);
    std::sort(result.begin(), result.end());

    ck_assert_int_eq(result.size(), result_size);
    auto key = parms.lower_bound;
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_eq(key++, result[i].key);
        if (key == 200) {
            key = 400;
        }
    }

    delete buffer1;
    delete buffer2;
}
END_TEST

START_TEST(t_lower_bound)
{
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    std::vector<Shard *> shards = {&shard1, &shard2};

    auto merged = Shard(shards);

    for (size_t i=100; i<1000; i++) {
        Rec r;
        r.key = i;
        r.value = i;

        auto idx = merged.get_lower_bound(i);

        assert(idx < merged.get_record_count());

        auto res = merged.get_record_at(idx);

        if (i >=200 && i <400) {
            ck_assert_int_lt(res->rec.key, i);
        } else {
            ck_assert_int_eq(res->rec.key, i);
        }
    }

    delete buffer1;
    delete buffer2;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Range Query Unit Testing");

    TCase *range_query = tcase_create("de:PGM::range_query Testing");
    tcase_add_test(range_query, t_range_query);
    tcase_add_test(range_query, t_buffer_range_query);
    tcase_add_test(range_query, t_range_query_merge);
    suite_add_tcase(unit, range_query);

    return unit;
}


int shard_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_shardner = srunner_create(unit);

    srunner_run_all(unit_shardner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_shardner);
    srunner_free(unit_shardner);

    return failed;
}


int main() 
{
    int unit_failed = shard_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}