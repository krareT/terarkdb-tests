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
    size_t indexId;
    size_t colgroupId;
public:
    TerarkBenchmark(Setting &setting1) : tab(NULL), Benchmark(setting1){};
    ~TerarkBenchmark() {
        assert(tab == NULL);
    }
private:
    ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichEPlan,
    std::atomic<std::vector<uint8_t >*>* whichSPlan) override;
    void PrintHeader();
    void Close() override;
    void Load(void) override;
    std::string getKey(std::string &str);
    void Open() override ;
    void DoWrite( bool seq) ;
    bool VerifyOneKey(llong rid,valvec<byte> &outside,DbContextPtr &ctx);
    bool ReadOneKey(ThreadState *thread) override;
    bool UpdateOneKey(ThreadState *thread) override ;
    bool InsertOneKey(ThreadState *thread) override ;
    bool Compact() override ;

    std::string HandleMessage(const std::string &msg) override;

    bool updateWriteThrottle(const std::string &val);

    std::string getWriteThrottle(void);
    bool updateColGroupMmapPopulate(const std::string &val);

    std::string getColGroupMmapPopulate(void);
    bool updateIndexMmapPopulate(const std::string &val);

    std::string getIndexMmapPopulate(void);
    bool updateCheckSumLevel(const std::string &val);

    std::string getCheckSumLevel(void);

    bool updateDictZipSampleRatio(const std::string &val);
    std::string getDictZipSampleRatio(void);

    bool updateMinMergeSetNum(const std::string &val);

    std::string getMinMergeSetNum(void);

    bool updatePurgeDeleteThreshold(const std::string &val);

    std::string getPurgeDeleteThreshold(void);

    bool updateMaxWritingSegmentSize(const std::string &val);

    std::string getMaxWritingSegmentSize(void);
};

#endif //TERARKDB_TEST_FRAMEWORK_TERARKBENCHMARK_H
