//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_STATS_H
#define TERARKDB_TEST_FRAMEWORK_STATS_H

#include "src/Setting.h"
#include <boost/circular_buffer.hpp>
#include <tbb/spin_rw_mutex.h>
#include <tbb/concurrent_queue.h>
#include <iostream>
#include <string>
class Stats {

private:
    typedef void (Stats::*Func_t)(struct timespec *start,struct timespec *end);
    const Func_t timeDataFuncMap[3];
    void readPlusOne(struct timespec *start,struct timespec *end);
    void updatePlusOne(struct timespec *start,struct timespec *end);
    void createPlusOne(struct timespec *start,struct timespec *end);
public:
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> readTimeDataCq;
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> createTimeDataCq;
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> updateTimeDataCq;
    Stats() : timeDataFuncMap {
                &Stats::readPlusOne,
                &Stats::createPlusOne,
                &Stats::updatePlusOne,
    } {}

    void FinishedSingleOp(BaseSetting::OP_TYPE type, struct timespec *start, struct timespec *end){
        (this->*timeDataFuncMap[int(type)])(start,end);
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_STATS_H
