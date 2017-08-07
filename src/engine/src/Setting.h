//
// Created by terark on 16-8-1.
//

#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <terark/fstring.hpp>
#include <terark/bitmap.hpp>

using terark::fstring;

struct PlanConfig{
    uint32_t read_percent = 100;
    uint32_t insert_percent = 0;
    uint32_t update_percent = 0;
    PlanConfig(){}
    PlanConfig(uint32_t r, uint32_t i, uint32_t u)
            : read_percent(r), insert_percent(i), update_percent(u) {}
};

enum class OP_TYPE{SEARCH, INSERT, UPDATE};
enum class VERIFY_TYPE{MATCH, MISMATCH, FAIL};

class BaseSetting{
public:
private:
    std::vector<PlanConfig> planConfigs;
    std::mutex planMtx;
    tbb::concurrent_unordered_map<uint32_t,uint32_t > threadPlanMap;
    std::atomic<bool > stop;
    std::atomic<uint8_t > compactTimes;
    std::atomic<uint8_t> threadNums;
    const
    std::unordered_map<std::string, bool (BaseSetting::*)(std::string&)> setFuncMap;
    std::atomic<uint8_t > samplingRate;
    std::string insertDataPath;
    std::string loadDataPath;
    std::string keysDataPath;
    std::string verifyKvFile;
    std::string action;
    tbb::concurrent_queue<std::string> message_cq;
    tbb::concurrent_queue<std::string> response_message_cq;

public:
    bool strSetInsertDataPath(std::string &);
    bool strSetSamplingRate(std::string &);
    bool strSetStop(std::string &);
    bool strSetReadPercent(std::string &);
    bool strSetThreadNums(std::string &);
    bool strSetInsertPercent(std::string &);
    bool strSetLoadDataPath(std::string &);
    bool setAction(std::string &);
    bool strSetKeysDataPath(std::string &);
    bool strSetVerifyKvFile(std::string &);
    bool strSetCompactTimes(std::string &);
    bool strSetMessage(std::string &);
    bool strSetPlanConfigs(std::string &);
    bool strSetThreadPlan(std::string&);

protected:

    void setThreadNums(uint8_t);

    std::string toString();

public:
    static std::string BenchmarkName;
    BaseSetting();
    BaseSetting (const BaseSetting&) = delete;
    uint8_t getSamplingRate(void) const ;
    uint8_t getCompactTimes(void) const ;
    uint32_t getThreadNums(void) const ;
    const std::string &getInsertDataPath(void) const ;
    const std::string &getLoadDataPath(void) const ;
    const std::string &getKeysDataPath(void) const ;
    const std::string &getVerifyKvFile(void) const;
    bool getPlanConfig(uint32_t, PlanConfig &planConfig);
    std::string getMessage(void);
    bool ifStop(void) const ;
    const std::string& getAction(void) const ;
    void setStop(void);
    std::string setBaseSetting(std::string &line);
    std::string setBaseSetting(int argc,char **argv);

    void sendMessageToSetting(const std::string &);
};
class Setting : public BaseSetting {
public:
    uint64_t FLAGS_block_size;
    uint32_t FLAGS_min_level_to_compress;
    uint64_t FLAGS_num_levels = 7; // rocksdb的level数量
    //BaseSetting baseSetting;
    const char* FLAGS_benchmarks =
                    "fillseq,"
                    "deleteseq,"
                    "fillseq,"
                    "deleterandom,"
                    "fillrandom,"
                    "deleteseq,"
                    "fillrandom,"
                    "deleterandom,"
                    "fillseqsync,"
                    "fillrandsync,"
                    "fillseq,"
                    "fillseqbatch,"
                    "fillrandom,"
                    "fillrandbatch,"
                    "overwrite,"
                    "readrandom,"
                    "readseq,"
                    "readreverse,";
     int FLAGS_num = 0;

// Number of read operations to do.  If negative, do FLAGS_num reads.
     int FLAGS_reads = -1;

// Number of concurrent threads to run.
     int FLAGS_threads = 1;

// Size of each value
     int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
     double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
// bool FLAGS_histogram = false;
     bool FLAGS_histogram = true;

     bool FLAGS_sync_index = true;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
     long FLAGS_write_buffer_size = 1L << 30;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
    long FLAGS_cache_size = -1;

    size_t skipInsertLines = 0;

// Maximum number of files to keep open at the same time (use default if == -1)
    int FLAGS_open_files = -1;

// Bloom filter bits per key.
// Negative means use default settings.
    int FLAGS_bloom_bits = -1;

    int target_file_size_multiplier = 2;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
    bool FLAGS_use_existing_db = false;

// Use the db with the following name.
    std::string FLAGS_db; // 数据库目录
    const char* FLAGS_resource_data = nullptr;
    const char* FLAGS_keys_data = nullptr;
    int *shuff = nullptr;

    bool FLAGS_use_lsm = false;
    bool FLAGS_stagger = false;
    bool disableWAL = false;
    bool autoSlowDownWrite = true;

    int FLAGS_max_compact_wait = 1200;
    int terocksdbIndexNestLevel = 3;

    size_t checksumLevel = 1;
    size_t terocksdbZipMinLevel = 0;
    size_t terocksdbZipWorkingMemSoftLimit = 16ull<<30; // 16G
    size_t terocksdbZipWorkingMemHardLimit = 32ull<<30; // 32G
    size_t terocksdbSmallTaskMemory = 1200ull<<20; // 1.2G
    double terocksdbSampleRatio = 0.015;
    double terocksdbIndexCacheRatio = 0.002;
    size_t numFields = size_t(-1);
    unsigned char fieldsDelim = '\t';
    std::vector<size_t> keyFields;

    std::vector<std::string> dbdirs; // 数据库目录
    std::string logdir;
    std::string waldir;
    std::string alt_engine_name;

    std::string terocksdb_tmpdir;
    size_t write_rate_limit = 30 << 20; // 30MB/s

    int flushThreads = 2;
    int compactThreads = 2;
    bool rocksdbUniversalCompaction = true;
    bool FLAGS_enable_auto_compact = true;
    bool FLAGS_use_mmap = false;
    const char* FLAGS_rocksdb_memtable = nullptr;
    size_t FLAGS_load_size = size_t(-1);

    Setting(int argc, char **argv);
    size_t splitKeyValue(fstring row, std::string* key, std::string* val) const;

private:
    terark::febitvec keyFieldsBits;
};
