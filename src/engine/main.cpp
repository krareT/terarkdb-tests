// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include <sys/types.h>
#include <sys/time.h>
#include <sstream>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "include/leveldb/cache.h"
#include "include/leveldb/db.h"
#include "include/leveldb/env.h"
#include "include/leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

#include <iostream>
#include <fstream>
#include <string.h>
#include <string>

//#include "stdafx.h"
#include <terark/db/db_table.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/RangeStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/autofree.hpp>
#include <terark/util/fstrvec.hpp>
#include <port/port_posix.h>
#include <src/Setting.h>
#include "src/leveldb.h"
#include "src/TerarkBenchmark.h"
#include "src/WiredTigerBenchmark.h"
// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)


// Number of key/values to place in database

//Setting setting;


int main(int argc, char** argv) {
    Setting setting;
    std::string default_db_path;

    for (int i = 1; i < argc; i++) {
        double d;
        int n;
        long size;
        char junk;
        if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
            setting.FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            setting.FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            setting.FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_lsm=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            setting.FLAGS_use_lsm = n;
        } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            setting.FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--max_compact_wait=%d%c", &n, &junk) == 1) {
            setting.FLAGS_max_compact_wait = n;
        } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
            setting.FLAGS_num = n;
        } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
            setting.FLAGS_reads = n;
        } else if (sscanf(argv[i], "--stagger=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            setting.FLAGS_stagger = n;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            setting.FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
            setting.FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
            setting.FLAGS_write_buffer_size = n;
        } else if (sscanf(argv[i], "--cache_size=%ld%c", &size, &junk) == 1) {
            std::cout << "cache_size " << size << std::endl;
            setting.FLAGS_cache_size = size;
        } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
            setting.FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
            setting.FLAGS_open_files = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            setting.FLAGS_db = argv[i] + 5;
        } else if (sscanf(argv[i], "--read_ratio=%lf%c", &d, &junk) == 1) {
            setting.FLAGS_read_write_percent = d;
        } else if (strncmp(argv[i], "--resource_data=", 16) == 0) {
            setting.FLAGS_resource_data = argv[i] + 16;
        } else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }
    }

    // Choose a location for the test database if none given with --db=<path>
    if (setting.FLAGS_db == NULL) {
        leveldb::Env::Default()->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        setting.FLAGS_db = default_db_path.c_str();
    }

    setting.shuff = (int *)malloc(setting.FLAGS_threads * sizeof(int));
    for (int i=0; i<setting.FLAGS_threads; i++)
        setting.shuff[i] = i;

    leveldb::WiredTIgerBenchmark benchmark(setting);
    benchmark.Run();
    return 0;
}
