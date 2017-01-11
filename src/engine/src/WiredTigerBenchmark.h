//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#include "Benchmark.h"

using namespace leveldb;
class WiredTigerBenchmark : public Benchmark{
private:
    WT_CONNECTION *conn_;
    std::string uri_;
    int db_num_;
    int num_;
    int sync_;

public:
    WiredTigerBenchmark(const Setting &setting1) : Benchmark(setting1) {
    }
    ~WiredTigerBenchmark() {
    }
private:
    size_t getKeyAndValue(std::string &str,std::string &key,std::string &val);
    ThreadState* newThreadState(std::atomic<std::vector<bool >*>* whichSPlan) override {
        return new ThreadState(threads.size(),conn_, nullptr,whichSPlan);
    }
    void Load(void) override{
        DoWrite(true); }
    void Close(void) override{
        conn_->close(conn_, NULL);
        conn_ = NULL;
    }
    bool ReadOneKey(ThreadState *thread) override;
    bool UpdateOneKey(ThreadState *thread) override;
    bool InsertOneKey(ThreadState *thread) override;
    void Open() override;
    bool Compact( ) override{
    };
    void DoWrite(bool seq) ;
    void PrintHeader() ;
    void PrintWarnings() ;
    void PrintEnvironment() ;
};


#endif //TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
