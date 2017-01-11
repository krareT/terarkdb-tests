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

#include <iostream>
#include <fstream>
#include <string.h>
#include <string>
#include <thread>
#include <tbb/concurrent_vector.h>
#include <terark/util/fstrvec.hpp>
#include "ThreadState.h"
#include "Setting.h"

//terarkdb -insert_data_path=/mnt/hdd/data/xab --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys
//terarkdb -update_data_path=/mnt/hdd/data/xab --benchmarks=fillrandom --num=45 --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys

class Benchmark : boost::noncopyable {
private:
    typedef bool (Benchmark::*executeFunc_t)(ThreadState *);
    typedef bool (Benchmark::*samplingFunc_t)(ThreadState *,BaseSetting::OP_TYPE);
    const executeFunc_t executeFuncMap[3]; // index is BaseSetting::OP_TYPE
    const samplingFunc_t samplingFuncMap[2]; // index is bool
    uint8_t samplingRecord[3] = {0, 0, 0}; // index is BaseSetting::OP_TYPE
    std::atomic<std::vector<uint8_t > *> executePlanAddr;
    std::atomic<std::vector<bool> *> samplingPlanAddr;
    std::vector<bool> samplingPlan[2];
    std::vector< std::pair<std::vector< uint8_t >,std::vector<uint8_t >>> executePlans;
    std::mt19937_64 random;
    bool whichEPlan = false;//不作真假，只用来切换plan
    bool whichSPlan = false;
    uint8_t compactTimes;
    void loadKeys(double keySampleRatio);
    void backupKeys(const std::string& fname);
    void loadInsertData(const Setting *setting);
    static void ThreadBody(Benchmark *bm,ThreadState* state){
        bm->ReadWhileWriting(state);
    }
    static void CompactThreadBody(Benchmark *bm){
        static std::mutex mtx;
        std::lock_guard<std::mutex> lock(mtx);
        bm->Compact();
    }
    void checkExecutePlan();
    void updatePlan(const PlanConfig &pc,std::vector<BaseSetting::OP_TYPE > &plan);
    void updateSamplingPlan(std::vector<bool> &plan, uint8_t percent);
    void shufflePlan(std::vector<uint8_t > &plan);
    void adjustThreadNum(uint32_t target, std::atomic<std::vector<bool > *> *whichSPlan);

    void adjustSamplingPlan(uint8_t samplingRate);
    void RunBenchmark(void);
    bool executeOneOperationWithSampling(ThreadState *state, BaseSetting::OP_TYPE type);
    bool executeOneOperationWithoutSampling(ThreadState *state, BaseSetting::OP_TYPE type);
    bool executeOneOperation(ThreadState* state,BaseSetting::OP_TYPE type);
    void ReadWhileWriting(ThreadState *thread);
    terark::fstrvec allkeys;
    tbb::spin_rw_mutex allkeysRwMutex;

    void reportMessage(const std::string &);
public:
    void Run(void);
    bool getRandomKey(std::string &key,std::mt19937_64 &rg);
    bool pushKey(std::string &key);
    std::vector<std::pair<std::thread,ThreadState*>> threads;
    Setting &setting;
    tbb::concurrent_queue<std::string> updateDataCq;

    Benchmark(const Setting&);
    virtual ~Benchmark();
    virtual void Open(void) = 0;
    virtual void Load(void) = 0;
    virtual void Close(void) = 0;
    virtual bool ReadOneKey(ThreadState*) = 0;
    virtual bool UpdateOneKey(ThreadState *) = 0;
    virtual bool InsertOneKey(ThreadState *) = 0;
    virtual ThreadState* newThreadState(std::atomic<std::vector<bool >*>* whichSamplingPlan) = 0;
    virtual bool Compact(void) = 0;

    virtual std::string HandleMessage(const std::string &msg);
};


#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
