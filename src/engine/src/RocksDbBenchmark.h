//
// Created by terark on 9/5/16.
//
#pragma once
#include "Benchmark.h"
#include <rocksdb/db.h>

class RocksDbBenchmark : public Benchmark {
private:
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    void setRocksDBOptions(Setting& set);
protected:
    rocksdb::DB *db;
    rocksdb::Options options;
public:
    ~RocksDbBenchmark();
    RocksDbBenchmark(Setting& set);
    void Open() override;
    void Load() override;
    void Close() override;
    bool ReadOneKey(ThreadState *) override;
    bool UpdateOneKey(ThreadState *) override;
    bool InsertOneKey(ThreadState *) override;
    bool VerifyOneKey(ThreadState *) override;
    ThreadState* newThreadState(const std::atomic<std::vector<bool>*> *whichSamplingPlan) override;
    bool Compact(void) override;
};

