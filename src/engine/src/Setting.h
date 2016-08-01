//
// Created by terark on 16-8-1.
//

#ifndef TERARKDB_TEST_FRAMEWORK_SETTING_H
#define TERARKDB_TEST_FRAMEWORK_SETTING_H


class Setting {
public:
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
     bool FLAGS_use_existing_db = true;

// Use the db with the following name.
     const char* FLAGS_db = nullptr;
     const char* FLAGS_db_table = nullptr;
     const char* FLAGS_resource_data = nullptr;

     int *shuff = nullptr;
};


#endif //TERARKDB_TEST_FRAMEWORK_SETTING_H
