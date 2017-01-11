//
// Created by terark on 9/5/16.
//
#pragma once
#include "Benchmark.h"
#include <rocksdb/db.h>

class RocksDbBenchmark : public Benchmark {
private:
    rocksdb::DB *db;
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    void setRocksDBOptions(const Setting &set);
protected:
    rocksdb::Options options;
public:
    ~RocksDbBenchmark();
    RocksDbBenchmark(const Setting &set);
    void Open() override;
    void Load() override;
    void Close() override;
    bool ReadOneKey(ThreadState *) override;
    bool UpdateOneKey(ThreadState *) override;
    bool InsertOneKey(ThreadState *) override;
    ThreadState *newThreadState(std::atomic<std::vector<bool > *> *whichSamplingPlan) override;
    bool Compact(void) override;
};

