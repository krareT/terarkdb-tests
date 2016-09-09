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
    std::unordered_map<uint8_t, bool (Benchmark::*)(ThreadState *)> executeFuncMap;
    std::unordered_map<uint8_t, bool (Benchmark::*)(ThreadState *,uint8_t)> samplingFuncMap;
    std::unordered_map<uint8_t ,uint8_t > samplingRecord;
    std::atomic<std::vector<uint8_t > *> executePlanAddr;
    std::atomic<std::vector<uint8_t > *> samplingPlanAddr;
    std::vector<uint8_t > executePlan[2];
    std::vector<uint8_t > samplingPlan[2];
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
    void updatePlan(std::vector<uint8_t> &plan, std::vector<std::pair<uint8_t ,uint8_t >> percent,uint8_t defaultVal);
    void shufflePlan(std::vector<uint8_t > &plan);
    void adjustThreadNum(uint32_t target, std::atomic<std::vector<uint8_t >*>* whichEPlan,
                         std::atomic<std::vector<uint8_t >*>* whichSPlan);
    void adjustExecutePlan(uint8_t readPercent,uint8_t insertPercent);
    void adjustSamplingPlan(uint8_t samplingRate);
    void RunBenchmark(void);
    bool executeOneOperationWithSampling(ThreadState* state,uint8_t type);
    bool executeOneOperationWithoutSampling(ThreadState* state,uint8_t type);
    bool executeOneOperation(ThreadState* state,uint8_t type);
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
    virtual ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichExecutePlan,
                                        std::atomic<std::vector<uint8_t >*>* whichSamplingPlan) = 0;
    virtual bool Compact(void) = 0;

    virtual std::string HandleMessage(const std::string &msg);
};


#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
