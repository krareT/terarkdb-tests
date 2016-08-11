//
// Created by terark on 16-8-11.
//

#include "Stats.h"
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::readTimeDataCq;
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::createTimeDataCq;
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::updateTimeDataCq;