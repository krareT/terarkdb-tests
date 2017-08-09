//
// Created by terark on 8/24/16.
//

#include "ThreadState.h"
#include <assert.h>

ThreadState::ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp)
  : tid(index), whichSamplingPlan(wsp) {
    STOP.store(false);
    seed = tid;
    randGenerator.seed(seed);
}

ThreadState::~ThreadState(){
}
