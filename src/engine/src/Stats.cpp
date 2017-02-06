//
// Created by terark on 16-8-11.
//

#include "Stats.h"
tbb::concurrent_queue<std::pair<uint64_t ,uint64_t >> Stats::opsDataCq[3];

void Stats::FinishedSingleOp(OP_TYPE type, const timespec& beg, const timespec& end) {
  opsDataCq[intptr_t(type)].push(std::make_pair(
      1000000000LL * beg.tv_sec + beg.tv_nsec,
      1000000000LL * end.tv_sec + end.tv_nsec)
      );
}
