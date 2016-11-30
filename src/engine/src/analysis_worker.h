//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
#define TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H

#include <thread>
#include <vector>
#include "Stats.h"

class AnalysisWorker {
private:
    Setting *setting;
    volatile bool shoud_stop = false;
public:
    std::string engine_name;
    AnalysisWorker(Setting *setting);

    ~AnalysisWorker();

    void run();

    void stop();
};

#endif //TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
