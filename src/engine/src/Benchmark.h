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
#include "terark/util/linebuf.hpp"
#include <port/port_posix.h>
#include <src/Setting.h>
#include <thread>
#include "src/leveldb.h"
#include <tbb/concurrent_vector.h>
#include <stdint.h>
#include <stdint-gcc.h>

//terarkdb -insert_data_path=/mnt/hdd/data/xab --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys
#include <fcntl.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>
//terarkdb -update_data_path=/mnt/hdd/data/xab --benchmarks=fillrandom --num=45 --sync_index=1 --db=./experiment/new_wiki --resource_data=/dev/stdin --threads=1 --keys_data=/home/terark/Documents/data/wiki_keys

using namespace terark;
using namespace db;
struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    Stats stats;
    std::atomic<uint8_t> STOP;
    WT_SESSION *session;
    std::atomic<std::vector<uint8_t >*> *whichExecutePlan;
    std::atomic<std::vector<uint8_t >*> *whichSamplingPlan;
    DbContextPtr ctx;
    ThreadState(int index,Setting &setting1,std::atomic<std::vector<uint8_t >*>* wep,
                std::atomic<std::vector<uint8_t >*>* wsp,DbTablePtr *tab)
            :   tid(index),
                whichExecutePlan(wep),
                whichSamplingPlan(wsp)
    {
        STOP.store(false);
        ctx = (*tab)->createDbContext();
        ctx->syncIndex = true;
    }
    ThreadState(int index,Setting &setting1,WT_CONNECTION *conn,
                std::atomic<std::vector<uint8_t >*>* wep,std::atomic<std::vector<uint8_t >*>* wsp)
            :tid(index),
             whichExecutePlan(wep),
             whichSamplingPlan(wsp){
        conn->open_session(conn, NULL, NULL, &session);
        STOP.store(false);
        assert(session != NULL);
    }
    ~ThreadState(){
    }
};
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
    size_t updateKeys(void);
    void backupKeys(void);

public:
    static const uint32_t FILE_BLOCK = 0;
    std::vector<std::pair<std::thread,ThreadState*>> threads;
    Setting &setting;
    static tbb::concurrent_queue<std::string> updateDataCq;
    static tbb::concurrent_vector<std::string> allkeys;
    static void loadInsertData(Setting *setting){
        FILE *ifs = fopen(setting->getInsertDataPath().c_str(),"r");
        assert(ifs != NULL);
        posix_fadvise(fileno(ifs),0,FILE_BLOCK,POSIX_FADV_SEQUENTIAL);
        std::string str;
        std::cout << "loadInsertData start" << std::endl;
        int count = 0;
        LineBuf line;
        while( setting->ifStop() == false && !feof(ifs)){
            while( updateDataCq.unsafe_size() < 200000){
                if (feof(ifs))
                    break;
                //fgets(buf,1024*1024,ifs);
                line.getline(ifs);
                str = line.p;
                updateDataCq.push(str);
                count ++;
            }
            std::cout << "insert : " << count << std::endl;
            count = 0;
            sleep(3);
        }
        fclose(ifs);
        std::cout << "loadInsertData stop" << std::endl;
    }
    Benchmark(Setting &s):setting(s){
        executeFuncMap[1] = &Benchmark::ReadOneKey;
        executeFuncMap[0] = &Benchmark::UpdateOneKey;
        executeFuncMap[2] = &Benchmark::InsertOneKey;
        samplingFuncMap[0] = &Benchmark::executeOneOperationWithoutSampling;
        samplingFuncMap[1] = &Benchmark::executeOneOperationWithSampling;
    };
    virtual void  Run(void) final {
        Open();

        std::cout << setting.ifRunOrLoad() << std::endl;
        if (setting.ifRunOrLoad() == "load") {
            allkeys.clear();
            Load();
            backupKeys();
        }
        else {
            std::cout << "allKeys size:" <<updateKeys() << std::endl;
            RunBenchmark();
        }
        Close();
    };

    virtual void Open(void) = 0;
    virtual void Load(void) = 0;
    virtual void Close(void) = 0;
    virtual bool ReadOneKey(ThreadState*) = 0;
    virtual bool UpdateOneKey(ThreadState *) = 0;
    virtual bool InsertOneKey(ThreadState *) = 0;
    virtual ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichExecutePlan,
                                        std::atomic<std::vector<uint8_t >*>* whichSamplingPlan) = 0;

    static void ThreadBody(Benchmark *bm,ThreadState* state){
        bm->ReadWhileWriting(state);
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

    std::string GatherTimeData();
};
class TerarkBenchmark : public Benchmark{
private:
    DbTablePtr tab;
public:
    TerarkBenchmark(Setting &setting1) : tab(NULL), Benchmark(setting1){};
    ~TerarkBenchmark() {
        tab->safeStopAndWaitForCompress();
        tab = NULL;
    }
private:
    ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichEPlan,
                                std::atomic<std::vector<uint8_t >*>* whichSPlan){
        return new ThreadState(threads.size(),setting,whichEPlan,whichSPlan,&tab);
    }
    void PrintHeader() {
        fprintf(stdout, "NarkDB Test Begins!");
    }
    void Close(){tab->safeStopAndWaitForFlush();tab = NULL;};
    void Load(void){

        DoWrite(true);
        //tab->compact();
    }
    std::string getKey(std::string &str){
        std::vector<std::string> strvec;
        boost::split(strvec,str,boost::is_any_of("\t"));
        return strvec[2] + '\0' + strvec[7];
    }
    std::string getKey(llong rid,const DbContextPtr &ctx,const std::vector<size_t> &idvec){
        std::string key;
        valvec<byte> row;
        for(auto eachId:idvec){
            row.clear();
            ctx->selectOneColumn(rid,eachId,&row);
            key += fstring(row).c_str();
            key += '\0';
        }
        //删除最后一个'\0'
        key.pop_back();
        std::cout << key << std::endl;
        return key;
    }
    void Open() {
        PrintHeader();
        assert(tab == NULL);
        std::cout << "Open database " << setting.FLAGS_db<< std::endl;
        tab = CompositeTable::open(setting.FLAGS_db);
        assert(tab != NULL);
    }
    void DoWrite( bool seq) {
        std::cout << "Write the data to terark : " << setting.getLoadDataPath() << std::endl;
        std::string str;
        long long recordnumber = 0;
        const Schema &rowSchema = tab->rowSchema();
        valvec <byte_t> row;
        DbContextPtr ctx = tab->createDbContext();
        ctx->syncIndex = setting.FLAGS_sync_index;
        FILE *loadFile = fopen(setting.getLoadDataPath().c_str(),"r");
        assert(loadFile != NULL);
        posix_fadvise(fileno(loadFile),0,FILE_BLOCK,POSIX_FADV_SEQUENTIAL);
        int temp = 200000;
        LineBuf line;
        while (line.getline(loadFile) && temp--) {
            str = line.p;
            if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', str, &row)) {
                std::cerr << "ERROR STR:" << str << std::endl;
                continue;
            }
            llong rid = ctx->upsertRow(row);
            if (rid < 0) { // unique index
                printf("Insert failed: %s\n", ctx->errMsg.c_str());
            }
            allkeys.push_back(getKey(str));
            assert(VerifyOneKey(rid, row, ctx) == true);
            recordnumber++;
            if (recordnumber % 100000 == 0)
                std::cout << "Insert reocord number: " << recordnumber / 10000 << "w" << std::endl;
        }
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("recordnumber %lld, time %s\n", recordnumber, asctime(timenow));
    }
    bool VerifyOneKey(llong rid,valvec<byte> &outside,DbContextPtr &ctx){
        valvec<byte > inside;
        ctx->getValue(rid,&inside);
        assert(inside.size() == outside.size());
        for(int i = 0; i < inside.size(); i ++){
            if (inside[i] != outside[i])
                return false;
        }
        return true;
    }
    bool ReadOneKey(ThreadState *thread) {
        if (allkeys.size() == 0) {
            std::cout << "allkeys empty" << std::endl;
            return false;
        }
        valvec<llong> idvec;
        size_t indexId = tab->getIndexId("cur_title,cur_timestamp");
        fstring key(allkeys.at(rand() % allkeys.size()));
        tab->indexSearchExact(indexId, key, &idvec,thread->ctx.get());
        //assert(idvec.size() <= 1);
        if ( idvec.size() == 0)
            return false;
        valvec<byte > row;
        thread->ctx->getValue(idvec[0],&row);
        return true;
    }
    bool UpdateOneKey(ThreadState *thread) {
        if (allkeys.size() == 0) {
            std::cout << "allkeys empty" << std::endl;
            return false;
        }
        valvec<llong> idvec;
        size_t indexId = tab->getIndexId("cur_title,cur_timestamp");
        fstring key(allkeys.at(rand() % allkeys.size()));
        tab->indexSearchExact(indexId, key, &idvec,thread->ctx.get());
        //assert(idvec.size() <= 1);
        if (idvec.size() == 0){
            return false;
        }
        valvec<byte> row;
        thread->ctx->getValue(idvec[0],&row);
        try {

            llong rid = thread->ctx->upsertRow(row);
            if (rid < 0) { // unique index
                printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
                return false;
            } else {
                assert(VerifyOneKey(rid, row, thread->ctx) == true);
            }
        }catch (NeedRetryException e){
            std::cerr << e.what() << std::endl;
            return false;
        }

        return true;
    }

    bool InsertOneKey(ThreadState *thread){
        std::string str;

        if (updateDataCq.try_pop(str) == false) {
            std::cerr << "cp empty!" << std::endl;
            return false;
        }
        const Schema &rowSchema = tab->rowSchema();
        valvec<byte_t> row;
        if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', str, &row)) {
            std::cerr << "InsertOneKey error:" << str << std::endl;
            return false;
        }
        if (thread->ctx->upsertRow(row) < 0) { // unique index
            printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
        allkeys.push_back(getKey(str));
        return true;
    }
};
using namespace leveldb;
class WiredTigerBenchmark : public Benchmark{
private:
    WT_CONNECTION *conn_;
    std::string uri_;
    int db_num_;
    int num_;
    int sync_;
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
    WiredTigerBenchmark(Setting &setting1) : Benchmark(setting1) {

    }
    ~WiredTigerBenchmark() {
    }
private:
    size_t getKeyAndValue(std::string &str,std::string &key,std::string &val){
        std::vector<std::string> strvec;
        boost::split(strvec,str,boost::is_any_of("\t"));
        key.clear();
        val.clear();
        assert(key.size() + val.size() == 0);
        if ( strvec.size() < 8)
            return 0;
        key = strvec[2] + strvec[7];
        for(int i = 0; i < strvec.size(); i ++){
            if (i == 2 || i == 7)
                continue;
            val.append(strvec[i]);
        }
        val = str;
        return strvec.size();
    }
    ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichEPlan,
                                std::atomic<std::vector<uint8_t >*>* whichSPlan){
        return new ThreadState(threads.size(),setting,conn_,whichEPlan,whichSPlan);
    }
    void Load(void){
        DoWrite(true);
    }
    void Close(void){
        conn_->close(conn_, NULL);
        conn_ = NULL;
    }
    bool ReadOneKey(ThreadState *thread) {

        WT_CURSOR *cursor;
        if ( allkeys.size() == 0){
            return false;
        }
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        int found = 0;
        std::string key(allkeys.at(rand() % allkeys.size()));
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

    bool UpdateOneKey(ThreadState *thread) {

        if (allkeys.size() == 0)
            return false;

        WT_CURSOR *cursor;
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL,NULL, &cursor);
        if (ret != 0){
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            return false;
        }
        std::string key = allkeys.at(rand() % allkeys.size());
        cursor->set_key(cursor, key.c_str());
        if (cursor->search(cursor) != 0){
            std::cerr << "cursor search error :" << key << std::endl;
            cursor->close(cursor);
            return false;
        }
        const char *val;
        ret = cursor->get_value(cursor,&val);
        assert(ret == 0);
        cursor->set_value(cursor,val);
        ret = cursor->insert(cursor);
        if (ret != 0){
            std::cerr << "cursor insert error :" << key << std::endl;
            cursor->close(cursor);
            return false;
        }
        cursor->close(cursor);
        return true;
    }
    bool InsertOneKey(ThreadState *thread){
        WT_CURSOR *cursor;
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL,NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        std::string str;
        if ( !updateDataCq.try_pop(str)) {
            cursor->close(cursor);
            return false;
        }
        std::string key;
        std::string val;

        ret = getKeyAndValue(str,key,val);
        if (ret == 0) {
            cursor->close(cursor);
            return false;
        }
        //allkeys.push_back(key);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor,val.c_str());

        ret = cursor->insert(cursor);
        if (ret != 0) {
            fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
            cursor->close(cursor);
            return false;
        }
        cursor->close(cursor);
        return true;
    }
    void Open() {
        PrintHeader();
        PrintEnvironment();
        PrintWarnings();
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
        std::cout << "DoWrite!" << std::endl;
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
        WT_SESSION *session;
        conn_->open_session(conn_, NULL, NULL, &session);
        int ret = session->open_cursor(session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }

        std::string str;
        long long recordnumber = 0;
        int temp = 200000;
        FILE *file = fopen(setting.getLoadDataPath().c_str(),"r");
        posix_fadvise(fileno(file),0,FILE_BLOCK,POSIX_FADV_SEQUENTIAL);
        LineBuf line;
        std::string key;
        std::string val;
        while(line.getline(file)&& temp --) {
            //寻找第二个和第三个\t
            str = line.p;
            ret = getKeyAndValue(str,key,val);
            assert(ret > 0);
            allkeys.push_back(key);
            cursor->set_key(cursor, key.c_str());
            cursor->set_value(cursor,val.c_str());
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

            cursor->close(cursor);
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("recordnumber %lld,  time %s\n",recordnumber, asctime(timenow));
    }
};
#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
