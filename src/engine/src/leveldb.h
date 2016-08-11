//
// Created by terark on 16-8-2.
//

#ifndef TERARKDB_TEST_FRAMEWORK_LEVELDB_H
#define TERARKDB_TEST_FRAMEWORK_LEVELDB_H
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

//#include "stdafx.h"
#include <terark/db/db_table.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/RangeStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/autofree.hpp>
#include <terark/util/fstrvec.hpp>
#include <port/port_posix.h>
#include <src/Setting.h>
#include <wiredtiger.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <time.h>
#include "tbb/spin_rw_mutex.h"
#include "Setting.h"
#include <boost/circular_buffer.hpp>
using namespace terark;
using namespace db;
class Stats {

private:
    const Setting &setting;
    boost::circular_buffer<std::pair<struct timespec,struct timespec>> readTimeData[2];
    boost::circular_buffer<std::pair<struct timespec,struct timespec>> updateTimeData[2];
    boost::circular_buffer<std::pair<struct timespec,struct timespec>> createTimeData[2];
    std::unordered_map<int, boost::circular_buffer<std::pair<struct timespec,struct timespec>>*> timeData;
    tbb::spin_rw_mutex timeDataRwLock;
    const uint64_t timeDataMax = 100000;
public:
    Stats(Setting &setting1) :setting(setting1){

        readTimeData[0].set_capacity(timeDataMax);
        readTimeData[1].set_capacity(timeDataMax);
        updateTimeData[0].set_capacity(timeDataMax);
        updateTimeData[1].set_capacity(timeDataMax);
        createTimeData[0].set_capacity(timeDataMax);
        createTimeData[1].set_capacity(timeDataMax);
        timeData[0] = updateTimeData;
        timeData[1] = readTimeData;
        timeData[2] = createTimeData;

    }
    std::string getTimeData(void) {
        std::stringstream ret;
        {
            tbb::spin_rw_mutex::scoped_lock lock(timeDataRwLock, true);
            updateTimeData[0].swap(updateTimeData[1]);
            readTimeData[0].swap(readTimeData[1]);
            createTimeData[0].swap(createTimeData[1]);
        }
        ret << "Update" << std::endl;
        for (auto &eachData : updateTimeData[1]) {
            auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
            ret << time << "\t";
            time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
            ret << time << std::endl;
        }
        ret << "Read" << std::endl;
        for (auto &eachData : readTimeData[1]) {
            auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
            ret << time << "\t";
            time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
            ret << time << std::endl;
        }
        ret << "Create" << std::endl;
        for (auto &eachData : createTimeData[1]) {
            auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
            ret << time << "\t";
            time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
            ret << time << std::endl;
        }
        createTimeData[1].clear();
        updateTimeData[1].clear();
        readTimeData[1].clear();

        return ret.str();
    }
    void FinishedSingleOp(unsigned char type, struct timespec *start, struct timespec *end){
        //读写锁
        timeDataRwLock.lock_read();
        timeData[type][0].push_back(std::make_pair(*start,*end));
        timeDataRwLock.unlock();
    }
};
struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    Stats *stats;
    std::atomic<uint8_t> STOP;
    WT_SESSION *session;
    std::atomic<std::vector<uint8_t >*> *which;
    ThreadState(int index,Setting &setting1,std::atomic<std::vector<uint8_t >*>* w)
            :   tid(index),
                which(w)
    {
        stats = new Stats(setting1);
        STOP.store(false);
    }
    ThreadState(int index,Setting &setting1,WT_CONNECTION *conn,std::atomic<std::vector<uint8_t >*>* w)
            :tid(index),which(w){

        stats = new Stats(setting1);
        conn->open_session(conn, NULL, NULL, &session);
        STOP.store(false);
        assert(session != NULL);
    }
    ~ThreadState(){
        delete(stats);
    }
};
namespace leveldb {

    namespace {

        static Slice TrimSpace(Slice s) {
            int start = 0;
            while (start < s.size() && isspace(s[start])) {
                start++;
            }
            int limit = s.size();
            while (limit > start && isspace(s[limit-1])) {
                limit--;
            }
            return Slice(s.data() + start, limit - start);
        }



// State shared by all concurrent executions of the same benchmark.

// Per-thread state for concurrent executions of the same benchmark.
    }  // namespace
}  // namespace leveldb


#endif //TERARKDB_TEST_FRAMEWORK_LEVELDB_H
