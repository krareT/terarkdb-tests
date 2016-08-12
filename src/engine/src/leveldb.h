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
#include <tbb/concurrent_queue.h>
#include "src/Stats.h"
using namespace terark;
using namespace db;



struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    Stats *stats;
    std::atomic<uint8_t> STOP;
    WT_SESSION *session;
    std::atomic<std::vector<uint8_t >*> *whichExecutePlan;
    std::atomic<std::vector<uint8_t >*> *whichSamplingPlan;

    ThreadState(int index,Setting &setting1,std::atomic<std::vector<uint8_t >*>* wep,
                std::atomic<std::vector<uint8_t >*>* wsp)
            :   tid(index),
                whichExecutePlan(wep),
                whichSamplingPlan(wsp)
    {
        stats = new Stats(setting1);
        STOP.store(false);
    }
    ThreadState(int index,Setting &setting1,WT_CONNECTION *conn,
                std::atomic<std::vector<uint8_t >*>* wep,std::atomic<std::vector<uint8_t >*>* wsp)
            :tid(index),
             whichExecutePlan(wep),
             whichSamplingPlan(wsp){
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
