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

class TerarkBenchmark {
private:
    CompositeTablePtr tab;

    fstrvec allkeys_;

    int num_;
    int value_size_;
    int entries_per_batch_;
    int reads_;
    int heap_counter_;
    Setting &setting;

    void PrintHeader() {
        fprintf(stdout, "NarkDB Test Begins!");
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

        Open();
        DoWrite(true);
        void (TerarkBenchmark::*method)(leveldb::ThreadState *) = NULL;

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
        leveldb::ThreadState *thread;

        void (TerarkBenchmark::*method)(leveldb::ThreadState *);
        ThreadArg(TerarkBenchmark *b,leveldb::ThreadState *t,void (TerarkBenchmark::*m)(leveldb::ThreadState *))
                :   bm(b),
                    thread(t),
                    method(m){
        }
    };

    static void ThreadBody(void *v) {
        ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
        leveldb::ThreadState *thread = arg->thread;

        thread->stats->Start();
        (arg->bm->*(arg->method))(thread);
        thread->stats->Stop();
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
    void RunTerarkBenchmark(int n, leveldb::Slice name,
                      void (TerarkBenchmark::*method)(leveldb::ThreadState *)) {
        leveldb::SharedState shared;
        shared.total = n;
        shared.num_initialized = 0;
        shared.num_done = 0;
        shared.start = false;
        shared.setting = & (this->setting);

        std::vector<std::pair<std::thread,ThreadArg*>> threads;
        int old_readPercent = -1;

        std::atomic<std::vector<uint8_t > *> planAddr;
        std::vector<uint8_t > plan[2];
        bool backupPlan = false;//不作真假，只用来切换plan
        leveldb::Stats::timeInit();
        while( !setting.baseSetting.ifStop()){



            int readPercent = setting.baseSetting.getReadPercent();
            if (readPercent != old_readPercent){
                //std::cout << "update Plan" << std::endl;
                old_readPercent = readPercent;
                updatePlan(plan[backupPlan],readPercent);
                planAddr.store( &(plan[backupPlan]));
                //std::cout <<" Main:" << planAddr.load() << std::endl;
                backupPlan = !backupPlan;
            }

            int threadNum = setting.baseSetting.getThreadNums();
            adjustThreadNum(threads,threadNum,&shared,method,&planAddr);
            gatherThreadInfo(threads);
            sleep(5);
        }
        adjustThreadNum(threads,0, nullptr, nullptr, nullptr);
    }


    void Open() {
        assert(tab == NULL);
        std::cout << "Create database " << setting.FLAGS_db << std::endl;

        tab = CompositeTable::createTable(setting.FLAGS_db_table.c_str());
        tab->load(setting.FLAGS_db);
    }

    void WriteSeq(leveldb::ThreadState *thread) {
        DoWrite(thread, true);
    }

    void WriteRandom(leveldb::ThreadState *thread) {
        DoWrite(thread, false);
    }

    void DoWrite(leveldb::ThreadState *thread, bool seq) {
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
        leveldb::TestRow recRow;
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
        std::cout << "writenum " << writen << ", avg " << copyavg
                  <<", offset" << offset << ", time " << asctime(timenow) << std::endl;
    }
    void DoWrite( bool seq) {
        DbContextPtr ctxw;
        ctxw = tab->createDbContext();
        ctxw->syncIndex = setting.FLAGS_sync_index;


        terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
        std::ifstream ifs(setting.FLAGS_resource_data);
        std::string str;

        //int64_t avg = FLAGS_num/FLAGS_threads;
        //int64_t copyavg = avg;
        /*int offset = shuff[thread->tid];
        int loop = 0;

        if (avg != FLAGS_num) {
            if (offset != 0) {
                int64_t skip = offset * avg;
                if (skip != 0) {
                    while(getline(ifs, str)) {
                        loop++;
                        if (loop == 15) {
                            loop = 0;
                            skip --;
                            if (skip == 0)
                                break;
                        }
                    }
                }
            }
        }
*/
        WikipediaRow recRow;
        int64_t writen = 0;
        long long linenumber = 0;
        long long recordnumber = 0;

        int total_line = setting.FLAGS_num;
        while(getline(ifs, str) && total_line != 0) {
            linenumber++;
            total_line--;
            if (writen == 0) {
                recRow.cur_id = lcast(str);
                // std::cout << "str " << recRow.cur_id << std::endl;
                writen++;
                continue;
            }
            if (writen == 1) {
                recRow.cur_namespace = lcast(str);
                // std::cout << "str " << recRow.cur_namespace << std::endl;
                writen++;
                continue;
            }
            if (writen == 2) {
                recRow.cur_title = str;
                // std::cout << "str " << recRow.cur_title << std::endl;
                writen++;
                continue;
            }
            if (writen == 3) {
                recRow.cur_text = str;
                // std::cout << "str " << recRow.cur_text << std::endl;
                writen++;
                continue;
            }
            if (writen == 4) {
                recRow.cur_comment = str;
                // std::cout << "str " << recRow.cur_comment << std::endl;
                writen++;
                continue;
            }
            if (writen == 5) {
                recRow.cur_user = lcast(str);
                // std::cout << "str " << recRow.cur_user << std::endl;
                writen++;
                continue;
            }
            if (writen == 6) {
                recRow.cur_user_text = str;
                // std::cout << "str " << recRow.cur_user_text << std::endl;
                writen++;
                continue;
            }
            if (writen == 7) {
                recRow.cur_timestamp = str;
                // std::cout << "str " << recRow.cur_timestamp << std::endl;
                writen++;
                continue;
            }
            if (writen == 8) {
                recRow.cur_restrictions = str;
                // std::cout << "str " << recRow.cur_restrictions << std::endl;
                writen++;
                continue;
            }
            if (writen == 9) {
                recRow.cur_counter = lcast(str);
                // std::cout << "str " << recRow.cur_counter << std::endl;
                writen++;
                continue;
            }
            if (writen == 10) {
                recRow.cur_is_redirect = lcast(str);
                // std::cout << "str " << recRow.cur_is_redirect << std::endl;
                writen++;
                continue;
            }
            if (writen == 11) {
                recRow.cur_minor_edit = lcast(str);
                // std::cout << "str " << recRow.cur_minor_edit << std::endl;
                writen++;
                continue;
            }
            if (writen == 12) {
                recRow.cur_random = str;
                // std::cout << "str " << recRow.cur_random << std::endl;
                writen++;
                continue;
            }
            if (writen == 13) {
                recRow.cur_touched = str;
                // std::cout << "str " << recRow.cur_touched << std::endl;
                writen++;
                continue;
            }
            if (writen == 14) {
                recRow.inverse_timestamp = str;
                // std::cout << "str " << recRow.inverse_timestamp << std::endl;
                rowBuilder.rewind();
                rowBuilder << recRow;
                fstring binRow(rowBuilder.begin(), rowBuilder.tell());
                if (ctxw->upsertRow(binRow) < 0) { // unique index
                    printf("Insert failed: %s\n", ctxw->errMsg.c_str());
                    exit(-1);
                }
                writen = 0;
                recordnumber++;
                continue;
            }
        }
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("linenumber %lld, recordnumber %lld, time %s\n",linenumber, recordnumber, asctime(timenow));

    }
    void DoWrite(leveldb::ThreadState *thread, bool seq,int times) {
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
        leveldb::TestRow recRow;
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



        bool ReadOneKey(leveldb::ThreadState *thread) {


            /*valvec <byte> keyHit, val;
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
            }*/

            valvec<byte> keyHit, val;
            valvec<valvec<byte> > cgDataVec;
            valvec<llong> idvec;
            valvec<size_t> colgroups;
            DbContextPtr ctxr;
            ctxr = tab->createDbContext();
            ctxr->syncIndex = setting.FLAGS_sync_index;
            // std::cout << " tab->getIndexNum() " << tab->getIndexNum() << " tab->getColgroupNum() " << tab->getColgroupNum() << std::endl;
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


            while (thread->STOP.load() == false) {

                std::vector<uint8_t > *plan = (*(thread->which)).load();
                for (auto each : *plan) {
                    (this->*func_map[each])(thread);
                    thread->stats->FinishedSingleOp(each);
                }
            }
            std::cout << "Thread " << thread->tid << " stop!" << std::endl;
        }
};


#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
