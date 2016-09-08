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
    std::unordered_map<uint8_t , void (Stats::*)(struct timespec *start,struct timespec *end)> timeDataFuncMap;
    void readPlusOne(struct timespec *start,struct timespec *end);
    void updatePlusOne(struct timespec *start,struct timespec *end);
    void createPlusOne(struct timespec *start,struct timespec *end);
public:
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> readTimeDataCq;
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> createTimeDataCq;
    static tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> updateTimeDataCq;
    Stats(){
        timeDataFuncMap[0] = &Stats::updatePlusOne;
        timeDataFuncMap[1] = &Stats::readPlusOne;
        timeDataFuncMap[2] = &Stats::createPlusOne;
    }

    void FinishedSingleOp(unsigned char type, struct timespec *start, struct timespec *end){
        (this->*timeDataFuncMap[type])(start,end);
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_STATS_H
