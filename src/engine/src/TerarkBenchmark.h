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
#include <thread>
#include "src/leveldb.h"
using namespace terark;
using namespace db;
class Benchmark{
public:
    virtual std::string GatherTimeData(void)= 0;
    virtual void Run(void) = 0;
};
class TerarkBenchmark : public Benchmark{
private:
    struct ThreadArg {
        TerarkBenchmark *bm;
        leveldb::ThreadState *thread;

        void (TerarkBenchmark::*method)(leveldb::ThreadState *);
        ThreadArg(TerarkBenchmark *b,leveldb::ThreadState *t,void (TerarkBenchmark::*m)(leveldb::ThreadState *))
                :   bm(b),
                    thread(t),
                    method(m){
        }
    };
    CompositeTablePtr tab;
    fstrvec allkeys_;

    int num_;
    int value_size_;
    int entries_per_batch_;
    int reads_;
    int heap_counter_;
    Setting &setting;
    std::vector<std::pair<std::thread,ThreadArg*>> threads;

    void PrintHeader() {
        fprintf(stdout, "NarkDB Test Begins!");
    }
public:
    std::string GatherTimeData(){

        static std::mutex mtx;
        std::stringstream ret;
        leveldb::Stats::changeWhich();
        for( auto& eachThread : threads){
            ret << "Thread " << eachThread.second->thread->tid << std::endl;
            ret << eachThread.second->thread->stats->getTimeData() << std::endl;
        }
        return ret.str();
    }
    TerarkBenchmark(Setting &setting1)
            : tab(NULL),
              num_(setting1.FLAGS_num),
              value_size_(setting1.FLAGS_value_size),
              entries_per_batch_(1),
              reads_(setting1.FLAGS_reads < 0 ? setting1.FLAGS_num : setting1.FLAGS_reads),
              heap_counter_(0),
              setting(setting1) {
        std::vector <std::string> files;
        leveldb::Env::Default()->GetChildren(setting.FLAGS_db, &files);
        for (int i = 0; i < files.size(); i++) {
            if (leveldb::Slice(files[i]).starts_with("heap-")) {
                leveldb::Env::Default()->DeleteFile(std::string(setting.FLAGS_db) + "/" + files[i]);
            }
        }
        if (!setting.FLAGS_use_existing_db) {
            leveldb::DestroyDB(setting.FLAGS_db, leveldb::Options());
        }
    }

    ~TerarkBenchmark() {
        tab->safeStopAndWaitForCompress();
        tab = NULL;
    }

    void Run() {
        PrintHeader();
        std::cout << " Run() " << std::endl;
        Open();
        std::ifstream ifs(setting.FLAGS_keys_data);
        std::string str;
        std::string key1;
        std::string key2;

        while (getline(ifs, str)) {
            allkeys_.push_back(str);
        }
        allkeys_.shrink_to_fit();
        printf("allkeys_.mem_size=%zd\n", allkeys_.full_mem_size());
        std::cout << allkeys_.size() << " " << setting.FLAGS_num << std::endl;
        assert(allkeys_.size() != 0);
        setting.FLAGS_num = allkeys_.size();
        //DoWrite(true);
        void (TerarkBenchmark::*method)(leveldb::ThreadState *) = &TerarkBenchmark::ReadWhileWriting;
        int num_threads = setting.FLAGS_threads;
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);
        RunTerarkBenchmark(num_threads, "RW", method);

        clock_gettime(CLOCK_MONOTONIC, &end);
        long long timeuse = 1000000000LL * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
        printf("RunTerarkBenchmark total time is : %lld \n", timeuse / 1000000000LL);
        //tab->syncFinishWriting();
        //tab->safeStopAndWaitForCompress();
        tab->safeStopAndWaitForFlush();
        allkeys_.erase_all();
    }

private:


    static void ThreadBody(void *v) {
        ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
        leveldb::ThreadState *thread = arg->thread;
        (arg->bm->*(arg->method))(thread);
    }
    void adjustThreadNum(   std::vector<std::pair<std::thread,ThreadArg*>> &threads,
                            uint32_t target,
                            leveldb::SharedState *shared,
                            void (TerarkBenchmark::*method)(leveldb::ThreadState *),
                            std::atomic<std::vector<uint8_t >*>* which){

        while (target > threads.size()){
            //Add new thread.
            ThreadArg *threadArg = new ThreadArg(this,new leveldb::ThreadState(threads.size(),setting,which),method);
            threads.push_back( std::make_pair(std::thread(ThreadBody,threadArg),threadArg));
        }
        while (target < threads.size()){
            //delete thread
            threads.back().second->thread->STOP.store(true);
            threads.back().first.join();
            delete threads.back().second->thread;
            delete threads.back().second;
            threads.pop_back();
        }
    }
    void adjustDataCapcity(uint64_t cap){
        for(auto& eachThread : threads){

        }
    }
    void RunTerarkBenchmark(int n, leveldb::Slice name,
                      void (TerarkBenchmark::*method)(leveldb::ThreadState *)) {
        leveldb::SharedState shared;
        shared.total = n;
        shared.num_initialized = 0;
        shared.num_done = 0;
        shared.start = false;
        shared.setting = & (this->setting);

        int old_readPercent = -1;

        std::atomic<std::vector<uint8_t > *> planAddr;
        std::vector<uint8_t > plan[2];
        bool backupPlan = false;//不作真假，只用来切换plan
        while( !setting.baseSetting.ifStop()){

            int readPercent = setting.baseSetting.getReadPercent();
            if (readPercent != old_readPercent){
                old_readPercent = readPercent;
                updatePlan(plan[backupPlan],readPercent);
                planAddr.store( &(plan[backupPlan]));
                backupPlan = !backupPlan;
            }
            int threadNum = setting.baseSetting.getThreadNums();
            adjustThreadNum(threads,threadNum,&shared,method,&planAddr);
            uint64_t data_cap = setting.baseSetting.getDataCapcity();
            adjustDataCapcity(data_cap);
            sleep(5);
        }
        adjustThreadNum(threads,0, nullptr, nullptr, nullptr);
    }

    void Open() {
        assert(tab == NULL);
        std::cout << "Open database " << setting.FLAGS_db<< std::endl;
        tab = CompositeTable::open(setting.FLAGS_db);
        assert(tab != NULL);
    }

    void DoWrite( bool seq) {
        std::cout << " Write the data to terark : " << setting.FLAGS_resource_data << std::endl;
        DbContextPtr ctxw;
        ctxw = tab->createDbContext();
        ctxw->syncIndex = setting.FLAGS_sync_index;

        terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
        std::ifstream ifs(setting.FLAGS_resource_data);
        std::string str;


        WikipediaRow recRow;
        int64_t writen = 0;
        long long linenumber = 0;
        long long recordnumber = 0;

        int total_line = setting.FLAGS_num;
        const Schema& rowSchema = tab->rowSchema();
        valvec<byte_t> row;
        while(getline(ifs, str) && total_line != 0) {
            linenumber++;
            total_line--;
            rowSchema.parseDelimText('\t', str, &row);

            if (ctxw->upsertRow(row) < 0) { // unique index
                printf("Insert failed: %s\n", ctxw->errMsg.c_str());
                exit(-1);
            }
            recordnumber++;
            if ( recordnumber % 100000 == 0)
                std::cout << "Insert reocord number: " << recordnumber << std::endl;
            //std::cout << "-";
        }
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("linenumber %lld, recordnumber %lld, time %s\n",linenumber, recordnumber, asctime(timenow));

    }
        bool ReadOneKey(leveldb::ThreadState *thread) {

            valvec<byte> keyHit, val;
            valvec<valvec<byte> > cgDataVec;
            valvec<llong> idvec;
            valvec<size_t> colgroups;
            DbContextPtr ctxr;
            ctxr = tab->createDbContext();
            ctxr->syncIndex = setting.FLAGS_sync_index;
            for (size_t i = tab->getIndexNum(); i < tab->getColgroupNum(); i++) {
                colgroups.push_back(i);
            }
            int found = 0;
            size_t indexId = 0;
            fstring key(allkeys_.at(rand() % allkeys_.size()));
            tab->indexSearchExact(indexId, key, &idvec, ctxr.get());
            for (auto recId : idvec) {
                tab->selectColgroups(recId, colgroups, &cgDataVec, ctxr.get());
            }
            if(idvec.size() > 0)
                found++;
            return found > 0;
        }

        bool WriteOneKey(leveldb::ThreadState *thread) {
            //DoWrite(thread,false,1);
            return true;
        }

        void updatePlan(std::vector<uint8_t> &plan, uint8_t readPercent) {
            assert(readPercent <= 100);
            if (plan.size() != 100){
                plan.resize(100);
            }
            std::fill_n(plan.begin(),readPercent,1);
            std::fill(plan.begin()+readPercent,plan.end(),0);
            std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
        }

        void ReadWhileWriting(leveldb::ThreadState *thread) {

            std::cout << "Thread " << thread->tid << " start!" << std::endl;
            std::unordered_map<uint8_t, bool (TerarkBenchmark::*)(leveldb::ThreadState *thread)> func_map;
            func_map[1] = &TerarkBenchmark::ReadOneKey;
            func_map[0] = &TerarkBenchmark::WriteOneKey;

            struct timespec start,end;
            while (thread->STOP.load() == false) {

                std::vector<uint8_t > *plan = (*(thread->which)).load();
                for (auto each : *plan) {
                    clock_gettime(CLOCK_REALTIME,&start);
                    if ((this->*func_map[each])(thread)) {
                        clock_gettime(CLOCK_REALTIME,&end);
                        thread->stats->FinishedSingleOp(each,&start,&end);
                    }
                }
            }
            std::cout << "Thread " << thread->tid << " stop!" << std::endl;
        }
};
using namespace leveldb;
class WiredTigerBenchmark : public Benchmark{
private:
    struct ThreadArg {
        WiredTigerBenchmark* bm;
        ThreadState* thread;
        void (WiredTigerBenchmark::*method)(ThreadState*);

        ThreadArg(WiredTigerBenchmark* bm0,ThreadState* ts,
                  void (WiredTigerBenchmark::*m)(ThreadState*))
                :   bm(bm0),
                    thread(ts),
                    method(m)
        {}
    };
    WT_CONNECTION *conn_;
    std::string uri_;
    std::string urii_;
    int db_num_;
    int num_;
    int value_size_;
    int entries_per_batch_;
    int sync_;
    int reads_;
    int heap_counter_;
    std::vector<std::pair<std::thread,ThreadArg*>> threads;
    std::vector<std::string> allkeys_;
    Setting &setting;
    void PrintHeader() {
        const int kKeySize = 16;
        PrintEnvironment();
        fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
                setting.FLAGS_value_size,
                static_cast<int>(setting.FLAGS_value_size * setting.FLAGS_compression_ratio + 0.5));
        fprintf(stdout, "Entries:    %d\n", num_);
        fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(kKeySize + setting.FLAGS_value_size) * num_)
                 / 1048576.0));
        fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
                (((kKeySize + setting.FLAGS_value_size * setting.FLAGS_compression_ratio) * num_)
                 / 1048576.0));
        PrintWarnings();
        fprintf(stdout, "------------------------------------------------\n");
    }

    void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        fprintf(stdout,
                "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
        );
#endif
#ifndef NDEBUG
        fprintf(stdout,
                "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

        // See if snappy is working by attempting to compress a compressible string
        const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        std::string compressed;
        if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
            fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
        } else if (compressed.size() >= sizeof(text)) {
            fprintf(stdout, "WARNING: Snappy compression is not effective\n");
        }
    }

    void PrintEnvironment() {
        int wtmaj, wtmin, wtpatch;
        const char *wtver = wiredtiger_version(&wtmaj, &wtmin, &wtpatch);
        fprintf(stdout, "WiredTiger:    version %s, lib ver %d, lib rev %d patch %d\n",
                wtver, wtmaj, wtmin, wtpatch);
        fprintf(stderr, "WiredTiger:    version %s, lib ver %d, lib rev %d patch %d\n",
                wtver, wtmaj, wtmin, wtpatch);

#if defined(__linux)
        time_t now = time(NULL);
        fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != NULL) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != NULL) {
                const char* sep = strchr(line, ':');
                if (sep == NULL) {
                    continue;
                }
                Slice key = TrimSpace(Slice(line, sep - 1 - line));
                Slice val = TrimSpace(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "cache size") {
                    cache_size = val.ToString();
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

public:
    std::string GatherTimeData(){

        static std::mutex mtx;
        std::stringstream ret;
        leveldb::Stats::changeWhich();
        for( auto& eachThread : threads){
            ret << "Thread " << eachThread.second->thread->tid << ":";
            ret << eachThread.second->thread->stats->getTimeData();
            ret << std::endl;
        }
        return ret.str();
    }
    WiredTigerBenchmark
            (Setting &setting1)
            : num_(setting1.FLAGS_num),
              value_size_(setting1.FLAGS_value_size),
              db_num_(0),
              entries_per_batch_(1),
              reads_(setting1.FLAGS_reads < 0 ? setting1.FLAGS_num : setting1.FLAGS_reads),
              heap_counter_(0),
              setting(setting1) {
        std::vector<std::string> files;
        Env::Default()->GetChildren(setting.FLAGS_db, &files);
        for (int i = 0; i < files.size(); i++) {
            if (Slice(files[i]).starts_with("heap-")) {
                Env::Default()->DeleteFile(std::string(setting.FLAGS_db) + "/" + files[i]);
            }
        }
        if (!setting.FLAGS_use_existing_db) {
            for (int i = 0; i < files.size(); i++) {
                std::string file_name(setting.FLAGS_db);
                file_name += "/";
                file_name += files[i];
                Env::Default()->DeleteFile(file_name.c_str());
            }
        }
    }

    ~WiredTigerBenchmark() {
    }

    void Run() {
        PrintHeader();
        std::cout << " Run() " << std::endl;

        /*std::ifstream ifs(setting.FLAGS_keys_data);
        std::string str;

        while(getline(ifs, str)) {
            allkeys_.push_back(str);
        }
        ifs.close();*/

        Open();
        DoWrite(true);
        allkeys_.shrink_to_fit();


        void (WiredTigerBenchmark::*method)(ThreadState *);


        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        method = &WiredTigerBenchmark::ReadWhileWriting;

        RunWiredTigerBenchmark(setting.baseSetting.getThreadNums(), "RWThread", method);

        clock_gettime(CLOCK_MONOTONIC, &end);
        long long timeuse = 1000000000 * ( end.tv_sec - start.tv_sec ) + end.tv_nsec -start.tv_nsec;
        printf("RunWiredTigerBenchmark total time is : %lld \n", timeuse/1000000000);

        allkeys_.clear();
        std::cout << "---------" << std::endl;
        if (conn_ != NULL) {
            conn_->close(conn_, NULL);
            conn_ = NULL;
        }
    }

private:

    static void ThreadBody(void *v) {
        ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
        leveldb::ThreadState *thread = arg->thread;

        (arg->bm->*(arg->method))(thread);
    }
    void adjustThreadNum(   std::vector<std::pair<std::thread,ThreadArg*>> &threads,
                            uint32_t target,
                            void (WiredTigerBenchmark::*method)(leveldb::ThreadState *),
                            std::atomic<std::vector<uint8_t >*>* which){

        while (target > threads.size()){
            //Add new thread.
            ThreadArg *threadArg = new ThreadArg(this,new leveldb::ThreadState(threads.size(),setting,conn_,which),method);
            threads.push_back( std::make_pair(std::thread(ThreadBody,threadArg),threadArg));
        }
        while (target < threads.size()){
            //delete thread
            threads.back().second->thread->STOP.store(true);
            threads.back().first.join();
            delete threads.back().second->thread;
            delete threads.back().second;
            threads.pop_back();
        }
    }
    void gatherThreadInfo(std::vector<std::pair<std::thread,ThreadArg*>> &threads){

        std::vector<uint64_t > readVec;
        std::vector<uint64_t > writeVec;
        uint64_t total_read = 0;
        uint64_t total_write = 0;
        std::cout << "------------------" << std::endl;

        for(auto& eachThreadInfo : threads){
            readVec.push_back(eachThreadInfo.second->thread->stats->typedDone_[1].exchange(0));
            writeVec.push_back(eachThreadInfo.second->thread->stats->typedDone_[0].exchange(0));
            total_read += readVec.back();
            total_write += writeVec.back();
        }
        for(int i = 0; i < readVec.size();i++){
            std::cout << "Thread " << i << " read " << readVec[i] << " write " << writeVec[i] << std::endl;
        }
        std::cout << "Total read " << total_read << " write " << total_write << std::endl;


    }
    void RunWiredTigerBenchmark(int n, leveldb::Slice name,
                                void (WiredTigerBenchmark::*method)(leveldb::ThreadState *)) {
        std::cout << "Run WiredTiger Benchmark!" << std::endl;
        leveldb::SharedState shared;
        shared.total = n;
        shared.num_initialized = 0;
        shared.num_done = 0;
        shared.start = false;
        shared.setting = & (this->setting);
        int old_readPercent = -1;
        std::atomic<std::vector<uint8_t > *> planAddr;
        std::vector<uint8_t > plan[2];
        bool backupPlan = false;//不作真假，只用来切换plan
        while( !setting.baseSetting.ifStop()){

            int readPercent = setting.baseSetting.getReadPercent();
            if (readPercent != old_readPercent){
                old_readPercent = readPercent;
                updatePlan(plan[backupPlan],readPercent);
                planAddr.store( &(plan[backupPlan]));
                backupPlan = !backupPlan;
            }
            int threadNum = setting.baseSetting.getThreadNums();
            adjustThreadNum(threads,threadNum,method,&planAddr);
            sleep(5);
            gatherThreadInfo(threads);
        }
        adjustThreadNum(threads,0,nullptr, nullptr);
    }
    bool ReadOneKey(ThreadState *thread) {

        WT_CURSOR *cursor;
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        int found = 0;
        std::string key(allkeys_.at(rand() % allkeys_.size()));
        cursor->set_key(cursor, key.c_str());
        if (cursor->search(cursor) == 0) {
            found++;
            const char* val;
            ret = cursor->get_value(cursor, &val);
            assert(ret == 0);
        }
        cursor->close(cursor);
        return found > 0;
    }

    bool WriteOneKey(ThreadState *thread) {

        return true;
    }

    void ReadWhileWriting(leveldb::ThreadState *thread) {

        std::cout << "Thread " << thread->tid << " start!" << std::endl;
        std::unordered_map<uint8_t, bool (WiredTigerBenchmark::*)(ThreadState *thread)> func_map;
        func_map[1] = &WiredTigerBenchmark::ReadOneKey;
        func_map[0] = &WiredTigerBenchmark::WriteOneKey;

        struct timespec start,end;
        while (thread->STOP.load() == false) {

            std::vector<uint8_t > *plan = (*(thread->which)).load();
            for (auto each : *plan) {
                clock_gettime(CLOCK_REALTIME,&start);
                if ((this->*func_map[each])(thread)) {
                    clock_gettime(CLOCK_REALTIME,&end);
                    thread->stats->FinishedSingleOp(each,&start,&end);
                }
            }
        }
        std::cout << "Thread " << thread->tid << " stop!" << std::endl;
    }


    /* Start Wired Tiger modified section. */
    void Open() {
#define SMALL_CACHE 10*1024*1024
        std::stringstream config;
        config.str("");
        if (!setting.FLAGS_use_existing_db) {
            config << "create";
        }
        if (setting.FLAGS_cache_size > 0)
            config << ",cache_size=" << setting.FLAGS_cache_size;
        config << ",log=(enabled,recover=on)";
        config << ",checkpoint=(log_size=64MB,wait=60)";
        /* TODO: Translate write_buffer_size - maybe it's chunk size?
           options.write_buffer_size = FLAGS_write_buffer_size;
           */
#ifndef SYMAS_CONFIG
        config << ",extensions=[libwiredtiger_snappy.so]";
#endif
        //config << ",verbose=[lsm]";
        Env::Default()->CreateDir(setting.FLAGS_db);
        wiredtiger_open(setting.FLAGS_db, NULL, config.str().c_str(), &conn_);
        assert(conn_ != NULL);

        WT_SESSION *session;
        conn_->open_session(conn_, NULL, NULL, &session);
        assert(session != NULL);

        char uri[100];
        snprintf(uri, sizeof(uri), "%s:dbbench_wt-%d",
                 setting.FLAGS_use_lsm ? "lsm" : "table", db_num_);
        uri_ = uri;

        if (!setting.FLAGS_use_existing_db) {
            // Create tuning options and create the data file
            config.str("");
            config << "key_format=S,value_format=S";
            config << ",columns=[p,val]";
            config << ",prefix_compression=true";
            config << ",checksum=off";

            if (setting.FLAGS_cache_size < SMALL_CACHE && setting.FLAGS_cache_size > 0) {
                config << ",internal_page_max=4kb";
                config << ",leaf_page_max=4kb";
                config << ",memory_page_max=" << setting.FLAGS_cache_size;
            } else {
                config << ",internal_page_max=16kb";
                config << ",leaf_page_max=16kb";
                if (setting.FLAGS_cache_size > 0) {
                    long memmax = setting.FLAGS_cache_size * 0.75;
                    // int memmax = setting.FLAGS_cache_size * 0.75;
                    config << ",memory_page_max=" << memmax;
                }
            }
            if (setting.FLAGS_use_lsm) {
                config << ",lsm=(";
                if (setting.FLAGS_cache_size > SMALL_CACHE)
                    config << ",chunk_size=" << setting.FLAGS_write_buffer_size;
                if (setting.FLAGS_bloom_bits > 0)
                    config << ",bloom_bit_count=" << setting.FLAGS_bloom_bits;
                else if (setting.FLAGS_bloom_bits == 0)
                    config << ",bloom=false";
                config << ")";
            }
#ifndef SYMAS_CONFIG
            config << ",block_compressor=snappy";
#endif
            fprintf(stderr, "Creating %s with config %s\n",uri_.c_str(), config.str().c_str());
            int ret = session->create(session, uri_.c_str(), config.str().c_str());
            if (ret != 0) {
                fprintf(stderr, "create error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            session->close(session, NULL);
        }
    }




    void DoWrite(bool seq) {

        std::stringstream txn_config;
        txn_config.str("");
        txn_config << "isolation=snapshot";
        if (sync_)
            txn_config << ",sync=full";
        else
            txn_config << ",sync=none";

        WT_CURSOR *cursor;
        std::stringstream cur_config;
        cur_config.str("");
        cur_config << "overwrite";
        if (seq && setting.FLAGS_threads == 1)
            cur_config << ",bulk=true";
        WT_SESSION *session;
        conn_->open_session(conn_, NULL, NULL, &session);
        int ret = session->open_cursor(session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        std::ifstream ifs(setting.FLAGS_resource_data);
        std::string str;
        int64_t writen = 0;
        long long recordnumber = 0;
        allkeys_.clear();
        while(getline(ifs, str)) {
            //寻找第二个和第三个\t
            size_t firstTab =  str.find('\t');
            assert(firstTab != std::string::npos);
            size_t secondTab = str.find('\t',firstTab + 1);
            assert(secondTab != std::string::npos);
            size_t thirdTab = str.find('\t',secondTab + 1);
            assert(thirdTab != std::string::npos);
            std::string key = str.substr(secondTab+1,thirdTab - secondTab - 1);
            allkeys_.push_back(key);
            cursor->set_key(cursor, key.c_str());
            str.erase(secondTab,thirdTab-secondTab);
            cursor->set_value(cursor,str.c_str());
            //std::cout << "key:" << key << std::endl;
            int ret = cursor->insert(cursor);
            if (ret != 0) {
                fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            recordnumber++;
            if (recordnumber % 100000 == 0)
                std::cout << "Record number: " << recordnumber << std::endl;
        }
        std::cout << allkeys_.size() << std::endl;
        cursor->close(cursor);
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("recordnumber %lld,  time %s\n",recordnumber, asctime(timenow));
    }

    void updatePlan(std::vector<uint8_t> &plan, uint8_t readPercent) {
        assert(readPercent <= 100);
        if (plan.size() != 100){
            plan.resize(100);
        }
        std::fill_n(plan.begin(),readPercent,1);
        std::fill(plan.begin()+readPercent,plan.end(),0);
        std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
