//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
#define TERARKDB_TEST_FRAMEWORK_THREADSTATE_H


#include <wiredtiger.h>
#include "Stats.h"
#include <terark/db/db_table.hpp>
#include <random>

struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    std::atomic<uint8_t> STOP;
    WT_SESSION *session;

    const std::atomic<std::vector<bool>*>* whichSamplingPlan;
    terark::db::DbContextPtr ctx;
    unsigned int seed;
    std::mt19937_64 randGenerator;
    std::string key;
    std::string value;
    std::string str;
    terark::valvec<terark::byte > row;
    terark::valvec<terark::llong> idvec;
    PlanConfig planConfig;
    std::vector<OP_TYPE> executePlan[2];
    std::atomic<uint32_t> whichPlan;
    uint32_t samplingRecord[3] = {0, 0, 0}; // index is OP_TYPE

    ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp, const terark::db::DbTablePtr& tab)
      : tid(index), whichSamplingPlan(wsp) {
        STOP.store(false);
        session = NULL;
        ctx = tab->createDbContext();
        ctx->syncIndex = true;
        seed = tid;
        randGenerator.seed(seed);
    }
    ThreadState(int index, WT_CONNECTION *conn, const std::atomic<std::vector<bool>*>* wsp)
      : tid(index), whichSamplingPlan(wsp){
        conn->open_session(conn, NULL, NULL, &session);
        STOP.store(false);
        assert(session != NULL);
        seed = tid;
        randGenerator.seed(seed);
    }
    ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp)
      : tid(index), whichSamplingPlan(wsp) {
        STOP.store(false);
        session = NULL;
        seed = tid;
        randGenerator.seed(seed);
    }
    ~ThreadState(){
    }
};


#endif //TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
