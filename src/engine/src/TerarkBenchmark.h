//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_TERARKBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_TERARKBENCHMARK_H

#include "terark/db/db_table.hpp"
#include "Benchmark.h"
using namespace terark;
using namespace db;
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
        return new ThreadState(threads.size(),whichEPlan,whichSPlan,&tab);
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
        posix_fadvise(fileno(loadFile),0,0,POSIX_FADV_SEQUENTIAL);
        char *buf;
        size_t n = 0;
        buf = NULL;
        std::string key;
        while (getline(&buf,&n,loadFile) != -1) {
            str = buf;
            if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', str, &row)) {
                std::cerr << "ERROR STR:" << str << std::endl;
                continue;
            }
            llong rid = ctx->upsertRow(row);
            if (rid < 0) { // unique index
                printf("Insert failed: %s\n", ctx->errMsg.c_str());
            }
            assert(VerifyOneKey(rid, row, ctx) == true);
            key = getKey(str);
            pushKey( key);
            recordnumber++;
            if (recordnumber % 100000 == 0)
                std::cout << "Insert reocord number: " << recordnumber / 10000 << "w" << std::endl;
            if (n > 1024*1024){
                free(buf);
                buf = NULL;
                n = 0;
            }
        }
        if (buf != NULL) {
            free(buf);
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
        std::string keyStr;
        if (false == getRandomKey(keyStr,thread->randGenerator)) {
            std::cout << "allkeys empty" << std::endl;
            return false;
        }
        thread->ctx;
        valvec<llong> idvec;
        size_t indexId = tab->getIndexId("cur_title,cur_timestamp");
        fstring key(keyStr);
        tab->indexSearchExact(indexId, key, &idvec,thread->ctx.get());
        //assert(idvec.size() <= 1);
        if ( idvec.size() == 0)
            return false;
        valvec<byte > row;
        thread->ctx->getValue(idvec[0],&row);
        return true;
    }
    bool UpdateOneKey(ThreadState *thread) {
        std::string keyStr;
        if (false == getRandomKey(keyStr,thread->randGenerator)) {
            std::cout << "allkeys empty" << std::endl;
            return false;
        }
        valvec<llong> idvec;
        size_t indexId = tab->getIndexId("cur_title,cur_timestamp");
        fstring key(keyStr);
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
            return false;
        }
        const Schema &rowSchema = tab->rowSchema();
        valvec<byte_t> row;
        if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', str, &row)) {
            std::cerr << "InsertOneKey error:" << str << std::endl;
            return false;
        }
        try{
            if (thread->ctx->upsertRow(row) < 0) { // unique index
                printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
                return false;
            }
        }catch (NeedRetryException e){
            std::cerr << e.what() << std::endl;
            return false;
        }
        std::string key = getKey(str);
        pushKey(key);
        return true;
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_TERARKBENCHMARK_H
