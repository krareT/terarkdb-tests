//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
#define TERARKDB_TEST_FRAMEWORK_THREADSTATE_H

#include "Stats.h"
#include <random>

struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    std::atomic<uint8_t> STOP;

    const std::atomic<std::vector<bool>*>* whichSamplingPlan;
    unsigned int seed;
    std::mt19937_64 randGenerator;
    std::string key;
    std::string value;
    std::string str;
    std::string storeValue;
    PlanConfig planConfig;
    std::vector<OP_TYPE> executePlan[2];
    std::atomic<uint32_t> whichPlan;
    uint32_t samplingRecord[3] = {0, 0, 0}; // index is OP_TYPE
    uint32_t verifySamplingRecord = 0;
    VERIFY_TYPE verifyResult = VERIFY_TYPE::FAIL;

    ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp);
    virtual ~ThreadState();
};

#endif //TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
