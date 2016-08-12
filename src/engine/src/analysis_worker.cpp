//
// Created by terark on 16-8-11.
//
#include "inttypes.h"
#include "analysis_worker.h"
#include "Stats.h"

void AnalysisWorker::run() {
    printf("Analysis worker is running ... \n");
    std::pair<uint64_t, uint64_t> result;
    while(true) {
        if(Stats::createTimeDataCq.try_pop(result)){
            uint64_t start = result.first;
            uint64_t end = result.second;
            printf("%" PRIu64 " -> %" PRIu64 "\n", start, end);
        }else{
            printf("Analysis worker sleep for 3 seconds\n");
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        }
    }
}