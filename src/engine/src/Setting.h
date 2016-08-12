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
    std::atomic<uint8_t > readPercent;
    std::atomic<uint8_t > stop;
    std::atomic<uint32_t> threadNums;
    std::unordered_map<std::string, bool (BaseSetting::*)(std::string&)> setFuncMap;
    std::atomic<uint8_t > samplingRate;
    enum{MAX_READ_PERCNT=100,MAX_THREAD_NUMS=100};
public:
    BaseSetting();
    BaseSetting (const BaseSetting&) = delete;
    enum READ_TYPE{
        READ_SEQ,
        READ_RANDOM
    };
    uint8_t getReadPercent(void);
    uint8_t getSamplingRate(void);
    uint32_t getThreadNums(void);
    bool ifStop(void);
    void setStop(void);

    void setReadPercent(uint8_t);
    void setThreadNums(uint32_t);
    std::string setBaseSetting(std::string &line);

    bool strSetSamplingRate(std::string&);
    bool strSetStop(std::string&);
    bool strSetReadPercent(std::string&);
    bool strSetThreadNums(std::string&);
    std::string toString();

};
class Setting : public BaseSetting{

public:
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
class TerarkSetting : private Setting{
private:
    std::unordered_map<
            std::string,
            std::pair<bool (TerarkSetting::*)(std::string&),
                    uint32_t (TerarkSetting::*)(void)>>  funcMap;
public:
    bool strSetCompressionRatio(std::string&);
    uint32_t getCompressionRatio(void);
    bool strSet(std::string&);
    int64_t strGet(std::string&);
    TerarkSetting(int argc,char **argv,char *name);
};

#endif //TERARKDB_TEST_FRAMEWORK_SETTING_H
