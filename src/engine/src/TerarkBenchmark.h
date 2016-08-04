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
#include "src/TcpServer.h"

using namespace terark;
using namespace db;
namespace leveldb {
    class TerarkBenchmark {
    private:
        CompositeTablePtr tab;

        fstrvec allkeys_;

        int num_;
        int value_size_;
        int entries_per_batch_;
        WriteOptions write_options_;
        int reads_;
        int heap_counter_;
        Setting &setting;

        void PrintHeader() {
            fprintf(stdout, "NarkDB Test Begins!");
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
            fprintf(stdout, "LevelDB:    version %d.%d\n",
                    kMajorVersion, kMinorVersion);
            fprintf(stderr, "LevelDB:    version %d.%d\n",
                    kMajorVersion, kMinorVersion);

#if defined(__linux)
            time_t now = time(NULL);
            fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

            FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
            if (cpuinfo != NULL) {
                char line[1000];
                int num_cpus = 0;
                std::string cpu_type;
                std::string cache_size;
                while (fgets(line, sizeof(line), cpuinfo) != NULL) {
                    const char *sep = strchr(line, ':');
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
        TerarkBenchmark(Setting &setting1)
                : tab(NULL),
                  num_(setting1.FLAGS_num),
                  value_size_(setting1.FLAGS_value_size),
                  entries_per_batch_(1),
                  reads_(setting1.FLAGS_reads < 0 ? setting1.FLAGS_num : setting1.FLAGS_reads),
                  heap_counter_(0),
                  setting(setting1) {
            std::vector <std::string> files;
            Env::Default()->GetChildren(setting.FLAGS_db, &files);
            for (int i = 0; i < files.size(); i++) {
                if (Slice(files[i]).starts_with("heap-")) {
                    Env::Default()->DeleteFile(std::string(setting.FLAGS_db) + "/" + files[i]);
                }
            }
            if (!setting.FLAGS_use_existing_db) {
                DestroyDB(setting.FLAGS_db, Options());
            }
        }

        ~TerarkBenchmark() {
            tab->safeStopAndWaitForCompress();
            tab = NULL;
        }

        void Run() {
            PrintHeader();
            std::cout << " Run() " << std::endl;

            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;
            std::string key1;
            std::string key2;

            while (getline(ifs, str)) {
                if (str.find("product/productId:") == 0) {
                    key1 = str.substr(19);
                    continue;
                }
                if (str.find("review/userId:") == 0) {
                    key2 = str.substr(15);
                    allkeys_.push_back(key1 + " " + key2);
                    continue;
                }
            }
            allkeys_.shrink_to_fit();
            printf("allkeys_.mem_size=%zd\n", allkeys_.full_mem_size());
            assert(allkeys_.size() == setting.FLAGS_num);

            Open();
            void (TerarkBenchmark::*method)(ThreadState *) = NULL;
            const char *benchmarks = setting.FLAGS_benchmarks;
            int num_threads = setting.FLAGS_threads;
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            method = &TerarkBenchmark::ReadWhileWriting;
            RunTerarkBenchmark(num_threads, "RW", method);

            clock_gettime(CLOCK_MONOTONIC, &end);
            long long timeuse = 1000000000LL * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
            printf("RunTerarkBenchmark total time is : %lld \n", timeuse / 1000000000LL);
            tab->syncFinishWriting();

            allkeys_.erase_all();
        }

    private:
        struct ThreadArg {
            TerarkBenchmark *bm;
            SharedState *shared;
            ThreadState *thread;

            void (TerarkBenchmark::*method)(ThreadState *);
            ThreadArg(TerarkBenchmark *b,SharedState *s,ThreadState *t,void (TerarkBenchmark::*m)(ThreadState *))
                    :   bm(b),
                        shared(s),
                        thread(t),
                        method(m){
                thread->shared = s;
            }
        };

        static void ThreadBody(void *v) {
            ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
            ThreadState *thread = arg->thread;

            thread->stats->Start();
            (arg->bm->*(arg->method))(thread);
            thread->stats->Stop();
        }
        void adjustThreadNum(   std::vector<std::pair<std::thread,ThreadArg*>> &threads,
                                uint32_t target,
                                SharedState *shared,
                                void (TerarkBenchmark::*method)(ThreadState *)){

            while (target > threads.size()){
                //Add new thread.
                ThreadArg *threadArg = new ThreadArg(this,shared,new ThreadState(threads.size(),setting),method);
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
        void RunTerarkBenchmark(int n, Slice name,
                          void (TerarkBenchmark::*method)(ThreadState *)) {
            SharedState shared;
            shared.total = n;
            shared.num_initialized = 0;
            shared.num_done = 0;
            shared.start = false;
            shared.setting = & (this->setting);

            std::vector<std::pair<std::thread,ThreadArg*>> threads;

            while( !setting.baseSetting.ifStop()){

                int threadNum = setting.baseSetting.getThreadNums();
                adjustThreadNum(threads,threadNum,&shared,method);

                sleep(5);
            }
            adjustThreadNum(threads,0,NULL,NULL);
        }

        void Crc32c(ThreadState *thread) {
            // Checksum about 500MB of data total
            const int size = 4096;
            const char *label = "(4K per op)";
            std::string data(size, 'x');
            int64_t bytes = 0;
            uint32_t crc = 0;
            while (bytes < 500 * 1048576) {
                crc = crc32c::Value(data.data(), size);
                thread->stats->FinishedSingleOp();
                bytes += size;
            }
            // Print so result is not dead
            fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

            thread->stats->AddBytes(bytes);
            thread->stats->AddMessage(label);
        }

        void AcquireLoad(ThreadState *thread) {
            int dummy;
            port::AtomicPointer ap(&dummy);
            int count = 0;
            void *ptr = NULL;
            thread->stats->AddMessage("(each op is 1000 loads)");
            while (count < 100000) {
                for (int i = 0; i < 1000; i++) {
                    ptr = ap.Acquire_Load();
                }
                count++;
                thread->stats->FinishedSingleOp();
            }
            if (ptr == NULL) exit(1); // Disable unused variable warning.
        }


        void Open() {
            assert(tab == NULL);
            std::cout << "Create database " << setting.FLAGS_db << std::endl;

            tab = CompositeTable::createTable(setting.FLAGS_db_table.c_str());
            tab->load(setting.FLAGS_db);
        }

        void WriteSeq(ThreadState *thread) {
            DoWrite(thread, true);
        }

        void WriteRandom(ThreadState *thread) {
            DoWrite(thread, false);
        }

        void DoWrite(ThreadState *thread, bool seq) {
            std::cout << " DoWrite now! num_ " << num_ << " setting.FLAGS_num " << setting.FLAGS_num << std::endl;

            DbContextPtr ctxw;
            ctxw = tab->createDbContext();
            ctxw->syncIndex = setting.FLAGS_sync_index;

            if (num_ != setting.FLAGS_num) {
                char msg[100];
                snprintf(msg, sizeof(msg), "(%d ops)", num_);
                thread->stats->AddMessage(msg);
            }


            terark::NativeDataOutput <terark::AutoGrownMemIO> rowBuilder;
            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;

            int64_t avg = setting.FLAGS_num / setting.FLAGS_threads;
            int64_t copyavg = avg;
            int offset = setting.shuff[thread->tid];
            if (avg != setting.FLAGS_num) {
                if (offset != 0) {
                    int64_t skip = offset * avg;
                    if (skip != 0) {
                        while (getline(ifs, str)) {
                            if (str.find("review/text:") == 0) {
                                skip--;
                                if (skip == 0)
                                    break;
                            }
                        }
                    }
                }
            }

            std::string key1;
            std::string key2;
            TestRow recRow;
            int64_t writen = 0;

            while (getline(ifs, str) && avg != 0) {
                fstring fstr(str);
                if (fstr.startsWith("product/productId:")) {
                    key1 = str.substr(19);
                    continue;
                }
                if (fstr.startsWith("review/userId:")) {
                    key2 = str.substr(15);
                    continue;
                }
                if (fstr.startsWith("review/profileName:")) {
                    recRow.profileName = str.substr(20);
                    continue;
                }
                if (fstr.startsWith("review/helpfulness:")) {
                    char *pos2 = NULL;
                    recRow.helpfulness1 = strtol(fstr.data() + 20, &pos2, 10);
                    recRow.helpfulness2 = strtol(pos2 + 1, NULL, 10);
                    continue;
                }
                if (fstr.startsWith("review/score:")) {
                    recRow.score = lcast(fstr.substr(14));
                    continue;
                }
                if (fstr.startsWith("review/time:")) {
                    recRow.time = lcast(fstr.substr(13));
                    continue;
                }
                if (fstr.startsWith("review/summary:")) {
                    recRow.summary = str.substr(16);
                    continue;
                }
                if (fstr.startsWith("review/text:")) {
                    recRow.text = str.substr(13);
                    recRow.product_userId = key1 + " " + key2;

                    rowBuilder.rewind();
                    rowBuilder << recRow;
                    fstring binRow(rowBuilder.begin(), rowBuilder.tell());

                    // if (ctxw->insertRow(binRow) < 0) { // non unique index
                    if (ctxw->upsertRow(binRow) < 0) { // unique index
                        printf("Insert failed: %s\n", ctxw->errMsg.c_str());
                        exit(-1);
                    }
                    thread->stats->FinishedSingleOp();
                    writen++;
                    avg--;
                    continue;
                }
            }
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
            printf("writenum %lld, avg %lld, offset %d, time %s\n", writen, copyavg, offset, asctime(timenow));
        }
        void DoWrite(ThreadState *thread, bool seq,int times) {
     //       std::cout << " DoWrite now! num_ " << num_ << " setting.FLAGS_num " << setting.FLAGS_num << std::endl;

            DbContextPtr ctxw;
            ctxw = tab->createDbContext();
            ctxw->syncIndex = setting.FLAGS_sync_index;

            if (num_ != setting.FLAGS_num) {
                char msg[100];
                snprintf(msg, sizeof(msg), "(%d ops)", num_);
                thread->stats->AddMessage(msg);
            }


            terark::NativeDataOutput <terark::AutoGrownMemIO> rowBuilder;
            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;

            int64_t avg = setting.FLAGS_num / setting.FLAGS_threads;
            int64_t copyavg = avg;
            int offset = setting.shuff[thread->tid];
            if (avg != setting.FLAGS_num) {
                if (offset != 0) {
                    int64_t skip = offset * avg;
                    if (skip != 0) {
                        while (getline(ifs, str)) {
                            if (str.find("review/text:") == 0) {
                                skip--;
                                if (skip == 0)
                                    break;
                            }
                        }
                    }
                }
            }

            std::string key1;
            std::string key2;
            TestRow recRow;
            int64_t writen = 0;

            while (getline(ifs, str) && times != 0) {
                times--;
                fstring fstr(str);
                if (fstr.startsWith("product/productId:")) {
                    key1 = str.substr(19);
                    continue;
                }
                if (fstr.startsWith("review/userId:")) {
                    key2 = str.substr(15);
                    continue;
                }
                if (fstr.startsWith("review/profileName:")) {
                    recRow.profileName = str.substr(20);
                    continue;
                }
                if (fstr.startsWith("review/helpfulness:")) {
                    char *pos2 = NULL;
                    recRow.helpfulness1 = strtol(fstr.data() + 20, &pos2, 10);
                    recRow.helpfulness2 = strtol(pos2 + 1, NULL, 10);
                    continue;
                }
                if (fstr.startsWith("review/score:")) {
                    recRow.score = lcast(fstr.substr(14));
                    continue;
                }
                if (fstr.startsWith("review/time:")) {
                    recRow.time = lcast(fstr.substr(13));
                    continue;
                }
                if (fstr.startsWith("review/summary:")) {
                    recRow.summary = str.substr(16);
                    continue;
                }
                if (fstr.startsWith("review/text:")) {
                    recRow.text = str.substr(13);
                    recRow.product_userId = key1 + " " + key2;

                    rowBuilder.rewind();
                    rowBuilder << recRow;
                    fstring binRow(rowBuilder.begin(), rowBuilder.tell());

                    // if (ctxw->insertRow(binRow) < 0) { // non unique index
                    if (ctxw->upsertRow(binRow) < 0) { // unique index
                        printf("Insert failed: %s\n", ctxw->errMsg.c_str());
                        exit(-1);
                    }
                    thread->stats->FinishedSingleOp();
                    writen++;
                    avg--;
                    continue;
                }
            }
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
     //       printf("writenum %lld, avg %lld, offset %d, time %s\n", writen, copyavg, offset, asctime(timenow));
        }

        void ReadSequential(ThreadState *thread) {
            fprintf(stderr, "ReadSequential not supported\n");
            return;
        }
        void ReadReverse(ThreadState *thread) {
            fprintf(stderr, "ReadReverse not supported\n");
            return;
        }

        void ReadRandom(ThreadState *thread) {
            valvec <byte> keyHit, val;
            valvec <valvec<byte>> cgDataVec;
            valvec <llong> idvec;
            valvec <size_t> colgroups;
            DbContextPtr ctxr;
            ctxr = tab->createDbContext();
            ctxr->syncIndex = setting.FLAGS_sync_index;
            // std::cout << " tab->getIndexNum() " << tab->getIndexNum() << " tab->getColgroupNum() " << tab->getColgroupNum() << std::endl;
            for (size_t i = tab->getIndexNum(); i < tab->getColgroupNum(); i++) {
                colgroups.push_back(i);
            }

            int *shuffr = NULL;
            shuffr = (int *) malloc(setting.FLAGS_num * sizeof(int));
            printf("Thread:%d ReadRandom. setting.FLAGS_num:%d allkeys_.size():%d reads_:%d\n",
                   thread->tid, setting.FLAGS_num, allkeys_.size(), reads_);
            for (int i = 0; i < setting.FLAGS_num; i++)
                shuffr[i] = i % allkeys_.size();
            thread->rand.Shuffle(shuffr, setting.FLAGS_num);

            int found = 0;
            size_t indexId = 0;

            for (size_t i = 0; i < reads_; ++i) {
                int k = shuffr[i % setting.FLAGS_num];
                //printf("Thread:%d ReadRandom. allkeys_.size():%d  k:%d\n",thread->tid,allkeys_.size(),k);

                fstring key(allkeys_.at(k));

                tab->indexSearchExact(indexId, key, &idvec, ctxr.get());
                for (auto recId : idvec) {
                    tab->selectColgroups(recId, colgroups, &cgDataVec, ctxr.get());
                }
                if (idvec.size() > 0)
                    found++;
                thread->stats->FinishedSingleOp();
            }
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats->AddMessage(msg);

        }


            void ReadOneKey(ThreadState *thread) {


                valvec <byte> keyHit, val;
                valvec <valvec<byte>> cgDataVec;
                valvec <llong> idvec;
                valvec <size_t> colgroups;
                DbContextPtr ctxr;
                ctxr = tab->createDbContext();
                ctxr->syncIndex = setting.FLAGS_sync_index;
                fstring key(allkeys_.at(rand()%allkeys_.size()));

                size_t indexId = 0;
                tab->indexSearchExact(indexId, key, &idvec, ctxr.get());
                for (auto recId : idvec) {
                    tab->selectColgroups(recId, colgroups, &cgDataVec, ctxr.get());
                }
            }

            void WriteOneKey(ThreadState *thread) {
                DoWrite(thread,false,1);
            }

            void updatePlan(std::vector<uint8_t> &plan, int readPercent,ThreadState *thread) {
                plan.clear();
                for (int i = 0; i < readPercent; i++) {
                    plan.push_back(1);
                }
                while(plan.size() < 100){
                    plan.push_back(0);
                }
                thread->rand.Shuffle(plan);
            }

            void ReadWhileWriting(ThreadState *thread) {

                std::cout << "Thread " << thread->tid << " start!" << std::endl;
                static std::unordered_map<uint8_t, void (TerarkBenchmark::*)(ThreadState *thread)> func_map;
                func_map[1] = &TerarkBenchmark::ReadOneKey;
                func_map[0] = &TerarkBenchmark::WriteOneKey;
                int readPercent;
                int old_readPercent = -1;
                std::vector<uint8_t> plan;
                bool STOP = false;
                while (thread->STOP.load() == false) {

                    //readPercent = setting.baseSetting.getReadPercent();
                    readPercent = thread->shared->setting->baseSetting.getReadPercent();
                    if (readPercent > 100) {
                        readPercent = 100;
                    }
                    if (old_readPercent != readPercent) {
                        old_readPercent = readPercent;
                        updatePlan(plan,readPercent,thread);
                    }
                    for (auto each : plan) {
                        (this->*func_map[each])(thread);
                        thread->stats->FinishedSingleOp(each);
                    }
                }
                std::cout << "Thread " << thread->tid << " stop!" << std::endl;
            }

        void ReadMissing(ThreadState *thread) {
            fprintf(stderr, "ReadMissing not supported\n");
            return;
        }

        void ReadHot(ThreadState *thread) {
            valvec <byte> val;
            llong recId;

            DbContextPtr ctxr;
            ctxr = tab->createDbContext();
            ctxr->syncIndex = setting.FLAGS_sync_index;
            const int range = (setting.FLAGS_num + 99) / 100;
            for (int i = 0; i < reads_; i++) {
                recId = thread->rand.Next() % range;
                ctxr->getValue(recId, &val);
                thread->stats->FinishedSingleOp();
            }
        }

        void SeekRandom(ThreadState *thread) {
            fprintf(stderr, "SeekRandom not supported\n");
            return;
        }

        void DoDelete(ThreadState *thread, bool seq) {
            fprintf(stderr, "DoDelete not supported\n");
            return;
        }

        void DeleteSeq(ThreadState *thread) {
            DoDelete(thread, true);
        }

        void DeleteRandom(ThreadState *thread) {
            DoDelete(thread, false);
        }

        void ReadWriteDel(ThreadState *thread) {
            if (thread->tid % 3 == 0) { // read
                ReadRandom(thread);
            } else if (thread->tid % 3 == 1) { // write
                int64_t num = 0;
                terark::NativeDataOutput <terark::AutoGrownMemIO> rowBuilder;
                DbContextPtr ctxw;
                ctxw = tab->createDbContext();
                ctxw->syncIndex = setting.FLAGS_sync_index;

                while (true) {
                    std::ifstream ifs(setting.FLAGS_resource_data);
                    std::string str;
                    std::string key1;
                    std::string key2;

                    TestRow recRow;

                    while (getline(ifs, str)) {
                        fstring fstr(str);
                        if (fstr.startsWith("product/productId:")) {
                            key1 = str.substr(19);
                        }
                        if (fstr.startsWith("review/userId:")) {
                            key2 = str.substr(15);
                        }
                        if (fstr.startsWith("review/profileName:")) {
                            recRow.profileName = str.substr(20);
                        }
                        if (fstr.startsWith("review/helpfulness:")) {
                            char *pos2 = NULL;
                            recRow.helpfulness1 = strtol(fstr.data() + 20, &pos2, 10);
                            recRow.helpfulness2 = strtol(pos2 + 1, NULL, 10);
                        }
                        if (fstr.startsWith("review/score:")) {
                            recRow.score = lcast(fstr.substr(14));
                        }
                        if (fstr.startsWith("review/time:")) {
                            recRow.time = lcast(fstr.substr(13));
                        }
                        if (fstr.startsWith("review/summary:")) {
                            recRow.summary = str.substr(16);
                        }
                        if (fstr.startsWith("review/text:")) {
                            recRow.text = str.substr(13);
                            recRow.product_userId = key1 + " " + key2;

                            rowBuilder.rewind();
                            rowBuilder << recRow;
                            fstring binRow(rowBuilder.begin(), rowBuilder.tell());

                            if (ctxw->insertRow(binRow) < 0) {
                                printf("Insert failed: %s\n", ctxw->errMsg.c_str());
                                exit(-1);
                            }
                            num++;
                        }

                        MutexLock l(&thread->shared->mu);
                        if (thread->shared->num_done + 2 * setting.FLAGS_threads / 3 >=
                            thread->shared->num_initialized) {
                            printf("extra write operations number %d\n", num);
                            return;
                        }
                    }
                }
            } else {  // del
                valvec <byte> keyHit, val;
                valvec <llong> idvec;
                DbContextPtr ctxr;
                ctxr = tab->createDbContext();
                ctxr->syncIndex = setting.FLAGS_sync_index;
                int64_t num = 0;

                size_t indexId = 0;
                while (true) {
                    const int k = thread->rand.Next() % setting.FLAGS_num;
                    fstring key(allkeys_.at(k));

                    tab->indexSearchExact(indexId, key, &idvec, ctxr.get());
                    for (auto recId : idvec) {
                        ctxr->removeRow(recId);
                    }
                    num++;
                    MutexLock l(&thread->shared->mu);
                    if (thread->shared->num_done + 2 * setting.FLAGS_threads / 3 >= thread->shared->num_initialized) {
                        printf("extra del operations number %d\n", num);
                        return;
                    }
                }
            }
        }


        void ReadWhileWritingNew4(ThreadState *thread) {
            int64_t readold = 0;
            int64_t readnew = 0;
            int64_t writenadd = 0;
            int64_t writenupdate = 0;
            valvec <valvec<byte>> cgDataVec;
            valvec <llong> idvec;
            valvec <size_t> colgroups;
            DbContextPtr ctxrw = tab->createDbContext();
            ctxrw->syncIndex = setting.FLAGS_sync_index;
            for (size_t i = tab->getIndexNum(); i < tab->getColgroupNum(); i++) {
                colgroups.push_back(i);
            }
            int found = 0;
            size_t indexId = 0;
            terark::NativeDataOutput <terark::AutoGrownMemIO> rowBuilder;
            terark::AutoFree<char> szPath;
            asprintf(&szPath.p, "%s.%d", setting.FLAGS_resource_data, thread->tid);
            std::ifstream ifs(szPath.p);
            std::string str;
            std::string key1;
            std::string key2;

            TestRow recRow;
            struct timespec start, end;
            long long readtime = 0;
            long long writetime = 0;
            long long totaltime = 0;
            double percent = setting.FLAGS_read_write_percent / 100.0;
            double percenr = setting.FLAGS_read_old_record_percent / 100.0;
            double percentw = setting.FLAGS_write_new_record_percent / 100.0;
            long long newrecord = 1;
            std::string record;
            std::stringstream recordstreamo;
            recordstreamo << newrecord;
            recordstreamo >> record;

            clock_gettime(CLOCK_MONOTONIC, &start);
            int i = 0;
            while (1) {
                size_t rdi = thread->rand.Next();
                double rdd = rdi / double(INT32_MAX);
                if (rdd < percent) {
                    size_t k = thread->rand.Next() % allkeys_.size();
                    size_t rdir = thread->rand.Next();
                    double rddr = rdir / double(INT32_MAX);
                    if (rddr < percenr) {
                        fstring key(allkeys_.at(k));
                        ctxrw->indexSearchExact(indexId, key, &idvec);
                        for (auto recId : idvec) {
                            ctxrw->selectColgroups(recId, colgroups, &cgDataVec);
                        }
                        if (idvec.size() > 0)
                            found++;
                        readold++;
                    } else {
                        std::string merge = allkeys_.at(k) + " " + record;
                        fstring key(merge);
                        ctxrw->indexSearchExact(indexId, key, &idvec);
                        for (auto recId : idvec) {
                            ctxrw->selectColgroups(recId, colgroups, &cgDataVec);
                        }
                        if (idvec.size() > 0)
                            found++;
                        readnew++;
                    }
                    thread->stats->FinishedSingleOp();
                } else {
                    size_t rdiw = thread->rand.Next();
                    double rddw = rdiw / double(INT32_MAX);
                    if (rddw < percentw) {   // add new record
                        while (getline(ifs, str)) {
                            fstring fstr(str);
                            if (fstr.startsWith("product/productId:")) {
                                key1 = str.substr(19);
                                continue;
                            }
                            if (fstr.startsWith("review/userId:")) {
                                key2 = str.substr(15);
                                continue;
                            }
                            if (fstr.startsWith("review/profileName:")) {
                                recRow.profileName = str.substr(20);
                                continue;
                            }
                            if (fstr.startsWith("review/helpfulness:")) {
                                char *pos2 = NULL;
                                recRow.helpfulness1 = strtol(fstr.data() + 20, &pos2, 10);
                                recRow.helpfulness2 = strtol(pos2 + 1, NULL, 10);
                                continue;
                            }
                            if (fstr.startsWith("review/score:")) {
                                recRow.score = lcast(fstr.substr(14));
                                continue;
                            }
                            if (fstr.startsWith("review/time:")) {
                                recRow.time = lcast(fstr.substr(13));
                                continue;
                            }
                            if (fstr.startsWith("review/summary:")) {
                                recRow.summary = str.substr(16);
                                continue;
                            }
                            if (fstr.startsWith("review/text:")) {
                                recRow.text = str.substr(13);
                                recRow.product_userId = key1 + " " + key2 + " " + record;
                                rowBuilder.rewind();
                                rowBuilder << recRow;
                                fstring binRow(rowBuilder.begin(), rowBuilder.tell());

                                if (ctxrw->upsertRow(binRow) < 0) { // unique index
                                    printf("Insert failed: %s\n", ctxrw->errMsg.c_str());
                                    continue;
                                }
                                writenadd++;
                                thread->stats->FinishedSingleOp();
                                break;
                            }
                        }
                    } else {   // update origin record
                        while (getline(ifs, str)) {
                            fstring fstr(str);
                            if (fstr.startsWith("product/productId:")) {
                                key1 = str.substr(19);
                                continue;
                            }
                            if (fstr.startsWith("review/userId:")) {
                                key2 = str.substr(15);
                                continue;
                            }
                            if (fstr.startsWith("review/profileName:")) {
                                recRow.profileName = str.substr(20);
                                continue;
                            }
                            if (fstr.startsWith("review/helpfulness:")) {
                                char *pos2 = NULL;
                                recRow.helpfulness1 = strtol(fstr.data() + 20, &pos2, 10);
                                recRow.helpfulness2 = strtol(pos2 + 1, NULL, 10);
                                continue;
                            }
                            if (fstr.startsWith("review/score:")) {
                                recRow.score = lcast(fstr.substr(14));
                                continue;
                            }
                            if (fstr.startsWith("review/time:")) {
                                recRow.time = lcast(fstr.substr(13));
                                continue;
                            }
                            if (fstr.startsWith("review/summary:")) {
                                recRow.summary = str.substr(16);
                                continue;
                            }
                            if (fstr.startsWith("review/text:")) {
                                recRow.text = str.substr(13);
                                recRow.product_userId = key1 + " " + key2;

                                rowBuilder.rewind();
                                rowBuilder << recRow;
                                fstring binRow(rowBuilder.begin(), rowBuilder.tell());

                                if (ctxrw->upsertRow(binRow) < 0) { // unique index
                                    printf("Insert failed: %s\n", ctxrw->errMsg.c_str());
                                    continue;
                                }
                                writenupdate++;
                                thread->stats->FinishedSingleOp();
                                break;
                            }
                        }
                    }
                    if (ifs.eof()) {
                        ifs.clear();
                        ifs.seekg(0, std::ios::beg);
                        newrecord++;
                        std::stringstream recordstream;
                        recordstream << newrecord;
                        recordstream >> record;
                    }
                }
                i++;
                if ((i + 1) % 1000 == 0) {
                    clock_gettime(CLOCK_MONOTONIC, &end);
                    long long timeuse = 1000000000LL * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
                    totaltime += timeuse;
                    //printf("i %d thread %d current qps %0.2f, total qps %0.2f, timeuse %f totaltime %f readold %lld readnew %lld writeadd %lld writeupdate %lld\n", i, thread->tid, 1000.0/(timeuse/1000000000.0), i/(totaltime/1000000000.0), timeuse/1000000000.0, totaltime/1000000000.0, readold, readnew, writenadd, writenupdate);
                    clock_gettime(CLOCK_MONOTONIC, &start);
                }

            }
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
        }


        void Compact(ThreadState *thread) {
            fprintf(stderr, "Compact not supported\n");
            return;
        }

        void PrintStats(const char *key) {
            fprintf(stderr, "PrintStats not supported\n");
            return;
        }

        static void WriteToFile(void *arg, const char *buf, int n) {
            reinterpret_cast<WritableFile *>(arg)->Append(Slice(buf, n));
        }

        void HeapProfile() {
            char fname[100];
            snprintf(fname, sizeof(fname), "%s/heap-%04d", setting.FLAGS_db, ++heap_counter_);
            WritableFile *file;
            Status s = Env::Default()->NewWritableFile(fname, &file);
            if (!s.ok()) {
                fprintf(stderr, "%s\n", s.ToString().c_str());
                return;
            }
            bool ok = port::GetHeapProfile(WriteToFile, file);
            delete file;
            if (!ok) {
                fprintf(stderr, "heap profiling not supported\n");
                Env::Default()->DeleteFile(fname);
            }
        }
    };
};

#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
