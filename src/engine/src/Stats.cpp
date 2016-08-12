//
// Created by terark on 16-8-11.
//

#include "Stats.h"
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::readTimeDataCq;
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::createTimeDataCq;
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::updateTimeDataCq;

void Stats::readPlusOne(struct timespec *start,struct timespec *end){
    readTimeDataCq.push(std::make_pair(
            1000000000LL * start->tv_sec + start->tv_nsec,
            1000000000LL * end->tv_sec + end->tv_nsec)
    );
}
void Stats::updatePlusOne(struct timespec *start,struct timespec *end){
    updateTimeDataCq.push(std::make_pair(
            1000000000LL * start->tv_sec + start->tv_nsec,
            1000000000LL * end->tv_sec + end->tv_nsec)
    );
}
void Stats::createPlusOne(struct timespec *start,struct timespec *end){
    createTimeDataCq.push(std::make_pair(
            1000000000LL * start->tv_sec + start->tv_nsec,
            1000000000LL * end->tv_sec + end->tv_nsec)
    );
}