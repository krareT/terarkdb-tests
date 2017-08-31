//
// Created by terark on 16-8-2.
//

#ifndef TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
#include <sys/types.h>
#include <sys/time.h>
#include <sstream>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <string>
#include <thread>
#include <tbb/concurrent_vector.h>
#include <terark/util/fstrvec.hpp>
#include <terark/util/autoclose.hpp>
#include "ThreadState.h"
#include "Setting.h"

//terarkdb -insert_data_path=/mnt/hdd/data/xab --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys
//terarkdb -update_data_path=/mnt/hdd/data/xab --benchmarks=fillrandom --num=45 --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys

class Benchmark : boost::noncopyable {
private:
    typedef bool (Benchmark::*executeFunc_t)(ThreadState *);
    const executeFunc_t executeFuncMap[3]; // index is OP_TYPE
    std::atomic<std::vector<uint8_t>*> executePlanAddr;
    std::atomic<std::vector<bool>*> samplingPlanAddr;
    std::vector<bool> samplingPlan[2];
    std::mt19937_64 random;
    uint64_t shufKeyIndex;
    bool whichSPlan = false;
    uint8_t compactTimes;
    terark::fstrvec allkeys;

    void loadKeys();
    void loadInsertData();
    void loadVerifyKvData();
    void loadShufKeyData();
    void checkExecutePlan();
    void updatePlan(const PlanConfig &pc,std::vector<OP_TYPE > &plan);
    void updateSamplingPlan(std::vector<bool> &plan, uint8_t percent);
    void shufflePlan(std::vector<uint8_t > &plan);
    void adjustThreadNum(uint32_t target, const std::atomic<std::vector<bool>*>* whichSPlan);
    void adjustVerifyThreadNum(uint32_t target, const std::atomic<std::vector<bool>*>* whichSPlan);
    void executeVerify(ThreadState *thread);
    void adjustSamplingPlan(uint8_t samplingRate);
    void RunBenchmark(void);
    void Verify(void);
    bool executeOneOperation(ThreadState* state,OP_TYPE type);
    void ReadWhileWriting(ThreadState *thread);
    void reportMessage(const std::string &);

    std::shared_ptr<terark::Auto_fclose> ifs;

protected:
    void clearThreads();
public:
    void Run(void);
    bool getRandomKey(std::string &key,std::mt19937_64 &rg);
    std::vector<std::pair<std::thread, ThreadState*>> threads;
    Setting& setting;
    tbb::concurrent_queue<std::string> updateDataCq;
    tbb::concurrent_queue<std::string> verifyDataCq;
    tbb::concurrent_queue<std::string> shufKeyDataCq;

    Benchmark(Setting&);
    virtual ~Benchmark();
    virtual void Open(void) = 0;
    virtual void Load(void) = 0;
    virtual void Close(void) = 0;
    virtual bool ReadOneKey(ThreadState*) = 0;
    virtual bool UpdateOneKey(ThreadState*) = 0;
    virtual bool InsertOneKey(ThreadState*) = 0;
    virtual bool VerifyOneKey(ThreadState *) = 0;
    virtual ThreadState* newThreadState(const std::atomic<std::vector<bool>*>* whichSamplingPlan) = 0;
    virtual bool Compact(void) = 0;

    virtual std::string HandleMessage(const std::string &msg);
};


#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
