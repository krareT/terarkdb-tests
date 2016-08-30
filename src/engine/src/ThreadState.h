//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
#define TERARKDB_TEST_FRAMEWORK_THREADSTATE_H


#include <wiredtiger.h>
#include "Stats.h"
#include "terark/db/db_table.hpp"

struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    Stats stats;
    std::atomic<uint8_t> STOP;
    WT_SESSION *session;
    std::atomic<std::vector<uint8_t >*> *whichExecutePlan;
    std::atomic<std::vector<uint8_t >*> *whichSamplingPlan;
    terark::db::DbContextPtr ctx;
    unsigned int seed;
    std::mt19937 randGenerator;
    std::string key;
    std::string value;
    std::string str;
    terark::valvec<terark::byte > row;
    terark::valvec<terark::llong> idvec;
    ThreadState(int index,std::atomic<std::vector<uint8_t >*>* wep,
    std::atomic<std::vector<uint8_t >*>* wsp,terark::db::DbTablePtr *tab)
    :   tid(index),
    whichExecutePlan(wep),
    whichSamplingPlan(wsp)
    {
        STOP.store(false);
        ctx = (*tab)->createDbContext();
        ctx->syncIndex = true;
        seed = tid;
        randGenerator.seed(seed);
    }
    ThreadState(int index,WT_CONNECTION *conn,
                std::atomic<std::vector<uint8_t >*>* wep,std::atomic<std::vector<uint8_t >*>* wsp)
    :tid(index),
    whichExecutePlan(wep),
    whichSamplingPlan(wsp){
        conn->open_session(conn, NULL, NULL, &session);
        STOP.store(false);
        assert(session != NULL);
        seed = tid;
        randGenerator.seed(seed);
    }
    ~ThreadState(){
    }
};


#endif //TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
