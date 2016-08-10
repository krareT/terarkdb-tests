//
// Created by terark on 16-8-1.
//

#ifndef TERARKDB_TEST_FRAMEWORK_SETTING_H
#define TERARKDB_TEST_FRAMEWORK_SETTING_H

#include <string>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <unordered_map>

class BaseSetting{
private:
    std::atomic<uint8_t > READ_PERCENT;
    std::atomic<uint8_t > STOP;
    std::atomic<uint32_t> THREAD_NUMS;
    std::atomic<uint64_t > data_capcity;
    std::unordered_map<std::string, bool (BaseSetting::*)(std::string&)> setFunc_map;
    const uint64_t TIME_DATA_MAX_CAPCITY = 10000000;

    enum{MAX_READ_PERCNT=100,MAX_THREAD_NUMS=100};
public:
    BaseSetting();
    BaseSetting (const BaseSetting&) = delete;
    std::string INIT_DATA_SOURCE;
    std::string WRITE_DATA_SOURCE;

    enum READ_TYPE{
        READ_SEQ,
        READ_RANDOM
    };
    double WRITE_INSERT_PERCENT;
    uint64_t OPS_RECORD_STEP;

    void setReadPercent(uint8_t);
    uint8_t getReadPercent(void);
    bool ifStop(void);
    void setStop(void);
    void setThreadNums(uint32_t);
    uint32_t getThreadNums(void);
    uint64_t getDataCapcity(void){ return data_capcity.load();};
    std::string setBaseSetting(std::string &line);
    std::string toString();

    bool strSetStop(std::string&);
    bool strSetReadPercent(std::string&);
    bool strSetThreadNums(std::string&);
    bool strSetTimeDataCapicity(std::string&);
};
class Setting{

public:
    BaseSetting baseSetting;
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
     int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
     int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
     int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
     int FLAGS_bloom_bits = -1;

// read write percent
     double FLAGS_read_write_percent = 100.0;
     double FLAGS_write_new_record_percent = 80.0;
     double FLAGS_read_old_record_percent = 50.0;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
     bool FLAGS_use_existing_db = false;

// Use the db with the following name.
     const char* FLAGS_db = nullptr;
     std::string FLAGS_db_table;
     const char* FLAGS_resource_data = nullptr;
    const char* FLAGS_keys_data = nullptr;
     int *shuff = nullptr;

    bool FLAGS_use_lsm = true;


// Stagger starting point of reads for sequential (or reverse).
    bool FLAGS_stagger = false;

// Stagger starting point of reads for sequential (or reverse).
    int FLAGS_max_compact_wait = 1200;



// Use the db with the following name.
    void terarkSetting(int argc,char **argv);
    void wiredTigerSetting(int argc,char **argv);
    Setting(int argc,char **argv,char *name);
};


#endif //TERARKDB_TEST_FRAMEWORK_SETTING_H
