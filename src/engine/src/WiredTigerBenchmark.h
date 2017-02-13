//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#include "Benchmark.h"

class WiredTigerBenchmark : public Benchmark{
private:
    WT_CONNECTION *conn_ = NULL;
    std::string uri_;
    int db_num_ = 0;
    int num_ = 0;
    int sync_ = 0;

public:
    WiredTigerBenchmark(Setting& setting1) : Benchmark(setting1) {
    }
    ~WiredTigerBenchmark() {
    }
private:
    size_t getKeyAndValue(std::string &str,std::string &key,std::string &val);
    ThreadState* newThreadState(const std::atomic<std::vector<bool>*>* whichSPlan) override {
        return new ThreadState(threads.size(),conn_, whichSPlan);
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
    bool Compact() override { return true; };
    void DoWrite(bool seq) ;
    void PrintHeader() ;
    void PrintWarnings() ;
    void PrintEnvironment() ;
};


#endif //TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
