//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
#define TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H

#include <thread>
#include <vector>
#include "Stats.h"
#include "mysql_driver.h"

class AnalysisWorker {
private:
    sql::Connection *conn;
    Setting *setting;
    volatile bool shoud_stop = false;
    std::string engine_name;
public:
    AnalysisWorker(std::string engine_name, Setting *setting);

    ~AnalysisWorker();

    void run();

    void stop();
};


class TimeBucket {
private:
    sql::Connection *conn = nullptr;
    uint64_t step_in_seconds = 10; // in seconds
    std::string engine_name = nullptr;
    const std::vector<std::string>& dbdirs;

    uint64_t current_bucket = 0;  // seconds
    int operation_count = 0;

    int findTimeBucket(uint64_t time);

    void upload(int bucket, int ops, int type, bool uploadExtraData);

public:
    TimeBucket(sql::Connection *const connection,
               const std::string& engineName,
               const std::vector<std::string>& dbdirs)
            : conn(connection),
              engine_name(engineName),
              dbdirs(dbdirs)
    {}

    void add(uint64_t start, uint64_t end, int sampleRate, int type, bool uploadExtraData);
};

#endif //TERARKDB_TEST_FRAMEWORK_ANALYSIS_THREAD_H
