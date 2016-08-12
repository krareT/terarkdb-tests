//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
#define TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H

#include <thread>
#include "Stats.h"

class AnalysisWorker{
private:
    std::thread worker;
    void poll();
    void analysis(uint64_t start, uint64_t end);
    void upload();
public:
    void run();
};
#endif //TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
