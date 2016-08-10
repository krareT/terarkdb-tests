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

        class Stats {

        private:
            double start_;
            double finish_;

            Histogram hist_;
            std::string message_;
            const Setting &setting;

            boost::circular_buffer<std::pair<struct timespec,struct timespec>> readTimeData[2];
            boost::circular_buffer<std::pair<struct timespec,struct timespec>> updateTimeData[2];
            boost::circular_buffer<std::pair<struct timespec,struct timespec>> createTimeData[2];
            std::unordered_map<int, boost::circular_buffer<std::pair<struct timespec,struct timespec>>*> timeData;
            static bool whichTimeData;//仅用来切换
            static tbb::spin_rw_mutex timeDataRwLock;
            const uint64_t timeDataMax = 10;
        public:
            static bool changeWhich( void){
                timeDataRwLock.lock();
                whichTimeData = !whichTimeData;
                timeDataRwLock.unlock();
            }
            std::atomic<uint64_t > typedDone_[2];//0:write 1:read
            Stats(Setting &setting1) :setting(setting1){

                readTimeData[0].set_capacity(timeDataMax);
                readTimeData[0].set_capacity(timeDataMax);
                updateTimeData[0].set_capacity(timeDataMax);
                updateTimeData[0].set_capacity(timeDataMax);
                createTimeData[0].set_capacity(timeDataMax);
                createTimeData[0].set_capacity(timeDataMax);
                timeData[0] = updateTimeData;
                timeData[1] = readTimeData;
                timeData[2] = createTimeData;

            }
            std::string getTimeData(void) {
                std::stringstream ret;
                timeDataRwLock.lock_read();
                ret << "Update" << std::endl;
                for (auto &eachData : updateTimeData[!whichTimeData]) {
                    auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
                    ret << time << "\t";
                    time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
                    ret << time << std::endl;
                }
                ret << "Read" << std::endl;
                for (auto &eachData : readTimeData[!whichTimeData]) {
                    auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
                    ret << time << "\t";
                    time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
                    ret << time << std::endl;
                }
                ret << "Create" << std::endl;
                for (auto &eachData : createTimeData[!whichTimeData]) {
                    auto time = eachData.first.tv_sec * 1000000000 + eachData.first.tv_nsec;
                    ret << time << "\t";
                    time = eachData.second.tv_sec * 1000000000 + eachData.second.tv_nsec;
                    ret << time << std::endl;
                }
                createTimeData[!whichTimeData].clear();
                updateTimeData[!whichTimeData].clear();
                readTimeData[!whichTimeData].clear();

                timeDataRwLock.unlock();
                return ret.str();
            }
            void FinishedSingleOp(unsigned char type, struct timespec *start, struct timespec *end){
                //读写锁
                timeDataRwLock.lock_read();
                timeData[type][whichTimeData].push_back(std::make_pair(*start,*end));
                timeDataRwLock.unlock();
            }
        };

// State shared by all concurrent executions of the same benchmark.
        struct SharedState {
            port::Mutex mu;
            port::CondVar cv;
            int total;
            Setting *setting;
            // Each thread goes through the following states:
            //    (1) initializing
            //    (2) waiting for others to be initialized
            //    (3) running
            //    (4) done

            int num_initialized;
            int num_done;
            bool start;
            SharedState() : cv(&mu) { }
        };

// Per-thread state for concurrent executions of the same benchmark.
        struct ThreadState {
            int tid;             // 0..n-1 when running in n threads
            Random rand;         // Has different seeds for different threads
            Stats *stats;
            SharedState* shared;
            std::atomic<uint8_t> STOP;
            WT_SESSION *session;
            std::atomic<std::vector<uint8_t >*> *which;
            ThreadState(int index,Setting &setting1,std::atomic<std::vector<uint8_t >*>* w)
                    :   tid(index),
                        rand(1000 + index),
                        which(w)
                        {
                stats = new Stats(setting1);
                STOP.store(false);
            }
            ThreadState(int index,Setting &setting1,WT_CONNECTION *conn,std::atomic<std::vector<uint8_t >*>* w)
            :tid(index),rand(index + 1000),which(w){

                stats = new Stats(setting1);
                conn->open_session(conn, NULL, NULL, &session);
                STOP.store(false);
                assert(session != NULL);
            }
            ~ThreadState(){
                delete(stats);
            }
        };
    }  // namespace

    bool leveldb::Stats::whichTimeData = false;
    tbb::spin_rw_mutex leveldb::Stats::timeDataRwLock;
}  // namespace leveldb
struct WikipediaRow {
    int32_t cur_id;
    int32_t cur_namespace;
    std::string cur_title;
    std::string cur_text;
    std::string cur_comment;
    uint64_t cur_user;
    std::string cur_user_text;
    std::string cur_timestamp;
    std::string cur_restrictions;
    int32_t	cur_counter;
    int32_t cur_is_redirect;
    int32_t cur_minor_edit;
    std::string cur_random;
    std::string cur_touched;
    std::string inverse_timestamp;

    DATA_IO_LOAD_SAVE(WikipediaRow,
                      &cur_id
                      &cur_namespace
                      &Schema::StrZero(cur_title)
                      &Schema::StrZero(cur_text)
                      &Schema::StrZero(cur_comment)
                      &cur_user
                      &Schema::StrZero(cur_user_text)
                      &Schema::StrZero(cur_timestamp)
                      &Schema::StrZero(cur_restrictions)
                      &cur_counter
                      &cur_is_redirect
                      &cur_minor_edit
                      &Schema::StrZero(cur_random)
                      &Schema::StrZero(cur_touched)
                      &Schema::StrZero(inverse_timestamp)
    )
};


#endif //TERARKDB_TEST_FRAMEWORK_LEVELDB_H
