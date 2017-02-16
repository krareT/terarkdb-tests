//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_STATS_H
#define TERARKDB_TEST_FRAMEWORK_STATS_H

#include "Setting.h"
#include <tbb/concurrent_queue.h>

class Stats {
public:
    static tbb::concurrent_queue<std::pair<uint64_t, uint64_t >> opsDataCq[3];
    static void FinishedSingleOp(OP_TYPE type, const timespec& beg, const timespec& end);
};

#endif //TERARKDB_TEST_FRAMEWORK_STATS_H
