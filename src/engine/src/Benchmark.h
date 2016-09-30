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
#include "db/db_impl.h"
#include "db/version_set.h"
#include "include/leveldb/cache.h"
#include "include/leveldb/db.h"
#include "include/leveldb/env.h"
#include "include/leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

#include <iostream>
#include <fstream>
#include <string.h>
#include <string>
#include "terark/db/db_table.hpp"
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/RangeStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/autofree.hpp>
#include <terark/util/fstrvec.hpp>
#include "ThreadState.h"
//#include "terark/util/linebuf.hpp"
#include <port/port_posix.h>
#include <src/Setting.h>
#include <thread>
#include "src/leveldb.h"
#include <tbb/concurrent_vector.h>
#include <stdint.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
//terarkdb -insert_data_path=/mnt/hdd/data/xab --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys
#include <fcntl.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>
//terarkdb -update_data_path=/mnt/hdd/data/xab --benchmarks=fillrandom --num=45 --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys



class Benchmark{
private:
    std::unordered_map<BaseSetting::OP_TYPE , bool (Benchmark::*)(ThreadState *),EnumClassHash> executeFuncMap;
    std::unordered_map<bool , bool (Benchmark::*)(ThreadState *,BaseSetting::OP_TYPE)> samplingFuncMap;
    std::unordered_map<BaseSetting::OP_TYPE ,uint8_t,EnumClassHash> samplingRecord;
    std::atomic<std::vector<uint8_t > *> executePlanAddr;
    std::atomic<std::vector<bool> *> samplingPlanAddr;
    std::vector<bool > samplingPlan[2];
    std::vector< std::pair<std::vector< uint8_t >,std::vector<uint8_t >>> executePlans;

    bool whichEPlan = false;//不作真假，只用来切换plan
    bool whichSPlan = false;
    uint8_t compactTimes;
    size_t updateKeys(void);
    void backupKeys(void);
    static void loadInsertData(const Setting *setting);
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
    static terark::fstrvec allkeys;
    static tbb::spin_rw_mutex allkeysRwMutex;

    void reportMessage(const std::string &);
public:
    void  Run(void);
    bool getRandomKey(std::string &key,std::mt19937 &rg);
    bool pushKey(std::string &key);
    std::vector<std::pair<std::thread,ThreadState*>> threads;
    Setting &setting;
    static tbb::concurrent_queue<std::string> updateDataCq;

    Benchmark(Setting &s);
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
