//
// Created by terark on 9/5/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_ROCKESDBBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_ROCKESDBBENCHMARK_H


#include "Benchmark.h"
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/cache.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

class RocksDbBenchmark : public Benchmark {
private:
    rocksdb::DB *db;
    rocksdb::Options options;
    std::shared_ptr<const rocksdb::FilterPolicy> filter_policy_;
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;

    size_t getKeyAndValue(std::string &str, std::string &key, std::string &val);

public:
    RocksDbBenchmark(const Setting &set);

    void Open() override;

    void Load() override;

    void Close() override;

    bool ReadOneKey(ThreadState *) override;

    bool UpdateOneKey(ThreadState *) override;

    bool InsertOneKey(ThreadState *) override;

    ThreadState *newThreadState(std::atomic<std::vector<uint8_t> *> *whichExecutePlan,
                                std::atomic<std::vector<uint8_t> *> *whichSamplingPlan) override;

    bool Compact(void) override;
};


#endif //TERARKDB_TEST_FRAMEWORK_ROCKESDBBENCHMARK_H
