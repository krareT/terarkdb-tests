//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
#define TERARKDB_TEST_FRAMEWORK_THREADSTATE_H

#include "Stats.h"
#include <random>

struct ThreadState {
    int tid;             // 0..n-1 when running in n threads // 线程id
    std::atomic<uint8_t> STOP; // 线程是否停止

    const std::atomic<std::vector<bool>*>* whichSamplingPlan; // 线程使用那个采样plan
    unsigned int seed; // 随机数生成种子
    std::mt19937_64 randGenerator; // 随机数生成器
    std::string key;
    std::string value;
    std::string str;
    std::string storeValue;
    PlanConfig planConfig; //计划设置，insert|update|read的比例
    std::vector<OP_TYPE> executePlan[2]; // 执行的plan具体设置
    std::atomic<uint32_t> whichPlan; // 选用哪个plan
    uint32_t samplingRecord[3] = {0, 0, 0}; // index is OP_TYPE // 采样记录,哪个操作执行了多少次
    uint32_t verifySamplingRecord = 0;
    VERIFY_TYPE verifyResult = VERIFY_TYPE::FAIL;

    ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp);
    virtual ~ThreadState();
};

#endif //TERARKDB_TEST_FRAMEWORK_THREADSTATE_H
