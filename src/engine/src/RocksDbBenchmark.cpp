//
// Created by terark on 9/5/16.
//

#include "RocksDbBenchmark.h"
#include <rocksdb/memtablerep.h>
#include <rocksdb/table.h>
#include <terark/util/linebuf.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/profiling.hpp>

using namespace terark;

void RocksDbBenchmark::Open() {
  const std::string& dbname = setting.FLAGS_db;
  printf("rocksdb::DB::Open(%s)\n", dbname.c_str());
  rocksdb::Status s = rocksdb::DB::Open(options, dbname, &db);
  if (!s.ok()) {
    fprintf(stderr, "FATAL: rocksdb::DB::Open(%s) = %s\n",
        dbname.c_str(), s.ToString().c_str());
    exit(1);
  }
}

RocksDbBenchmark::~RocksDbBenchmark() {}
RocksDbBenchmark::RocksDbBenchmark(const Setting &set) : Benchmark(set) {
    db = nullptr;
    options.create_if_missing = true;
// new features to add
    options.allow_concurrent_memtable_write = true;
    options.enable_write_thread_adaptive_yield = true;
    options.allow_mmap_reads = true;
    options.allow_mmap_writes = true;
// end

    write_options.disableWAL = set.disableWAL;

    static const std::map<char, ullong> kmgtp = {
            {'k', 1024ull},
            {'K', 1024ull},
            {'m', 1024ull*1024},
            {'M', 1024ull*1024},
            {'g', 1024ull*1024*1024},
            {'G', 1024ull*1024*1024},
            {'t', 1024ull*1024*1024*1024},
            {'T', 1024ull*1024*1024*1024},
            {'p', 1024ull*1024*1024*1024*1024},
            {'P', 1024ull*1024*1024*1024*1024},
    };
    valvec<fstring> dircaps;
    fstring(setting.FLAGS_db).split(',', &dircaps);
    if (dircaps.size() > 1) {
        setting.dbdirs.clear();
        for (size_t i = 0; i < dircaps.size(); ++i) {
            fstring dircap = dircaps[i];
            const char* colon = dircap.strstr(":");
            if (colon) {
                char* suffix = NULL;
                double cap = strtof(colon+1, &suffix);
                auto iter = kmgtp.find(*suffix);
                if (kmgtp.end() != iter) {
                    cap *= iter->second;
                }
                std::string dir(dircap.data(), colon);
                options.db_paths.push_back({dir, uint64_t(cap)});
                setting.dbdirs.push_back(dir);
                fprintf(stderr, "RocksDB: add dbdir: cap=%6.1fG, %s\n", cap/(1ull<<30), dir.c_str());
            }
            else {
                fprintf(stderr, "ERROR: invalid dir:cap,...: %s\n", setting.FLAGS_db.c_str());
                exit(1);
            }
        }
        setting.FLAGS_db = std::string("multi-dir-") + setting.BenchmarkName;
    }
    if (setting.logdir.size()) {
        options.db_log_dir = setting.logdir;
        setting.dbdirs.push_back(setting.logdir);
    }
    if (setting.waldir.size()) {
        options.wal_dir = setting.waldir;
        setting.dbdirs.push_back(setting.waldir);
    }
    if (setting.getAction() == "load") {
        options.allow_concurrent_memtable_write = false;
        if (setting.FLAGS_rocksdb_memtable) {
            if (strcmp(setting.FLAGS_rocksdb_memtable, "vector") == 0) {
                options.memtable_factory.reset(new rocksdb::VectorRepFactory());
                fprintf(stderr, "RocksDB: use VectorRepFactory\n");
            }
            else {
                fprintf(stderr, "ERROR: invalid rocksdb_memtable(canbe vector or <empty>): %s\n",
                        setting.FLAGS_rocksdb_memtable);
                exit(1);
            }
        }
        if (!setting.FLAGS_enable_auto_compact) {
            options.disable_auto_compactions = true;
        }
    }
    else {
        if (setting.FLAGS_rocksdb_memtable) {
             fprintf(stderr, "INFO: ignore rocksdb_memtable: %s\n", setting.FLAGS_rocksdb_memtable);
        }
        if (!setting.FLAGS_enable_auto_compact) {
            fprintf(stderr, "INFO: ignore enable_auto_compact=0\n");
        }
    }

    if ("rocksdb" == set.BenchmarkName) {
      setRocksDBOptions(set);
    }

    options.max_open_files = set.FLAGS_open_files;
    options.base_background_compactions = set.compactThreads;
    options.max_background_compactions = set.compactThreads*2;
    options.num_levels = set.FLAGS_num_levels;
    options.env->SetBackgroundThreads(set.flushThreads, rocksdb::Env::HIGH);
    if (set.rocksdbUniversalCompaction) {
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
        options.compaction_options_universal.allow_trivial_move = true;
    //  options.compaction_options_universal.size_ratio = 10; // 10%
    }
    options.max_background_flushes = set.flushThreads;
    options.max_subcompactions = 2;
    options.max_write_buffer_number = 3;
    options.write_buffer_size = set.FLAGS_write_buffer_size;

    if ("load" == setting.action) {
      options.max_write_buffer_number = 6;
    }

    if (setting.write_rate_limit) {
        // limit write rate being stable
        options.delayed_write_rate = setting.write_rate_limit;
        options.max_write_buffer_number = 5;
        options.level0_slowdown_writes_trigger = 2;
        options.level0_stop_writes_trigger = 5000; // never stop write
    //  options.soft_pending_compaction_bytes_limit = setting.write_rate_limit * 60; // 1 minutes written bytes
        options.soft_pending_compaction_bytes_limit = 4ull << 40;
        options.hard_pending_compaction_bytes_limit = 8ull << 40;
        fprintf(stderr, "INFO: rocksdb set option.delayed_write_rate = %zd\n", setting.write_rate_limit);
    }
    else if (!setting.autoSlowDownWrite) {
        options.level0_slowdown_writes_trigger = 1000;
        options.level0_stop_writes_trigger = 1000;
        options.soft_pending_compaction_bytes_limit = 2ull << 40;
        options.hard_pending_compaction_bytes_limit = 4ull << 40;
    }
}

void RocksDbBenchmark::setRocksDBOptions(const Setting &set) {
  using rocksdb::BlockBasedTableOptions;
  BlockBasedTableOptions bbo;
  bbo.index_type = BlockBasedTableOptions::kBinarySearch;
  if (setting.FLAGS_cache_size > 0) {
    bbo.block_cache = rocksdb::NewLRUCache(setting.FLAGS_cache_size);
  }
  bbo.block_size = set.FLAGS_block_size;

  if (set.FLAGS_bloom_bits) {
    bbo.filter_policy = rocksdb::NewBloomFilterPolicy(set.FLAGS_bloom_bits, false);
  }
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbo));

  if (set.FLAGS_min_level_to_compress >= 0) {
      assert(set.FLAGS_min_level_to_compress <= set.FLAGS_num_levels);
      options.compression_per_level.resize(set.FLAGS_num_levels);
      for (int i = 0; i < set.FLAGS_min_level_to_compress; i++) {
          options.compression_per_level[i] = rocksdb::kNoCompression;
      }
      for (int i = set.FLAGS_min_level_to_compress;
           i < set.FLAGS_num_levels; i++) {
          options.compression_per_level[i] = rocksdb::kSnappyCompression;
      }
  }
  for (int i = 0; i < options.compression_per_level.size(); i++) {
      printf("options.compression_per_level[%d]=%d\n", i, options.compression_per_level[i]);
  }
}

void RocksDbBenchmark::Close() {
    delete db;
    db = NULL;
}

extern bool g_upload_fake_ops;
void RocksDbBenchmark::Load() {
    printf("RocksDbBenchmark Loading ...\n"); fflush(stdout);
    Auto_fclose loadFile(fopen(setting.getLoadDataPath().c_str(), "r"));
    if (loadFile == NULL) {
        fprintf(stderr, "load error : can not open file %s , check --load_data_path\n", setting.getLoadDataPath().c_str());
        exit(1);
    }
    g_upload_fake_ops = true;
    assert(loadFile != NULL);
    LineBuf line;
    std::string key, value;
    size_t lines = 0, last_lines = 0;
    size_t bytes = 0, last_bytes = 0;
    size_t limit = setting.FLAGS_load_size;
    profiling pf;
    long long t0 = pf.now();
    long long t1 = t0;
    while (bytes < limit && !feof(loadFile)) {
        size_t i = 0;
        for (; i < 100000 && bytes < limit && line.getline(loadFile) > 0; ++i) {
            line.chomp();
            if (setting.splitKeyValue(line, &key, &value) == 0)
                continue;
            rocksdb::Status s = db->Put(write_options, key, value);
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            }
            bytes += line.size();
            pushKey(key);
        }
        lines += i;
        long long t2 = pf.now();
        printf("line:%zuw, bytes:%9.3fG, records/sec: { cur = %6.2fM  avg = %6.2fM }, bytes/sec: { cur = %6.2fM  avg = %6.2fM }\n"
            , i / 10000, bytes / 1e9
            , (lines - last_lines) / pf.uf(t1, t2), lines / pf.uf(t0, t2)
            , (bytes - last_bytes) / pf.uf(t1, t2), bytes / pf.uf(t0, t2)
        );
        fflush(stdout);
        t1 = t2;
        last_lines = lines;
        last_bytes = bytes;
    }
    printf("RocksDbBenchmark Load done, total = %zd lines, %.3f GB, start compacting ...\n"
        , lines, bytes/1e9);
    fflush(stdout);
    db->CompactRange(NULL, NULL);
    long long t2 = pf.now();
    printf("RocksDbBenchmark compact done: total = %zd lines, size = %.3f GB\n"
        "  load    time = %8.2f seconds, speed = %8.3f MB/s\n"
        "  compact time = %8.2f seconds, speed = %8.3f MB/s\n"
        "  overall time = %8.2f seconds, speed = %8.3f MB/s\n"
        , lines, bytes/1e9
        , pf.sf(t0,t1), bytes/pf.uf(t0,t1)
        , pf.sf(t1,t2), bytes/pf.uf(t1,t2)
        , pf.sf(t0,t2), bytes/pf.uf(t0,t2)
        );
    fflush(stdout);
    extern void stop_test();
    stop_test();
}

bool RocksDbBenchmark::ReadOneKey(ThreadState *ts) {
    if (!getRandomKey(ts->key, ts->randGenerator))
        return false;
    if (!db->Get(read_options, ts->key, &(ts->value)).ok())
        return false;
    return true;
}

bool RocksDbBenchmark::UpdateOneKey(ThreadState *ts) {
    if (!getRandomKey(ts->key, ts->randGenerator)) {
     	fprintf(stderr,"RocksDbBenchmark::UpdateOneKey:getRandomKey false\n");
	    return false;
    }
    if (!db->Get(read_options, ts->key, &(ts->value)).ok()) {
    // 	fprintf(stderr,"RocksDbBenchmark::UpdateOneKey:db-Get false, value.size:%05zd, key:%s\n"
    //            , ts->value.size(), ts->key.c_str());
	    return false;
    }
    auto status = db->Put(write_options, ts->key, ts->value);
    if (false == status.ok()){
     	fprintf(stderr,"RocksDbBenchmark::UpdateOneKey:db-Put false\n key:%s\nvalue:%s\n",ts->key.c_str(),ts->value.c_str());
    	fprintf(stderr,"RocksDbBenchmakr::UpdateOneKey:db-Put status:%s\n",status.ToString().c_str());
	    return false;
    }
    fflush(stderr);
    return true;
}

bool RocksDbBenchmark::InsertOneKey(ThreadState *ts) {
    if (!updateDataCq.try_pop(ts->str)){
	    return false;
    }
    auto ret = setting.splitKeyValue(ts->str, &ts->key, &ts->value);
    if (ret == 0){
     	fprintf(stderr,"RocksDbBenchmark::InsertOneKey:getKeyAndValue false\n");
	    return false;
    }
    auto status = db->Put(write_options, ts->key, ts->value);
    if (!status.ok()){
     	fprintf(stderr,"RocksDbBenchmark::InsertOneKey:db->Put false.\nkey:%s\nvalue:%s\n",ts->key.c_str(),ts->value.c_str());
    	fprintf(stderr,"RocksDbBenchmakr::InsertOneKey:db-Put status:%s\n",status.ToString().c_str());
	    updateDataCq.push(ts->str);
      	return false;
    }
    pushKey(ts->key);
    return true;
}

bool RocksDbBenchmark::Compact(void) {
    size_t numLevels = size_t(setting.FLAGS_num_levels);
    fprintf(stderr
        , "RocksDbBenchmark::Compact(): levels = %zd ...\n"
        , numLevels
        );
    terark::profiling pf;
    long long t0 = pf.now();
    rocksdb::CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    cro.target_level = numLevels - 1;
    db->CompactRange(cro, NULL, NULL);
    long long t1 = pf.now();
    fprintf(stderr
        , "RocksDbBenchmark::Compact(): levels = %zd done, time = %f sec!\n"
        , numLevels, pf.sf(t0,t1)
        );
    return false;
}

ThreadState *RocksDbBenchmark::newThreadState(std::atomic<std::vector<bool > *> *whichSamplingPlan) {
    return new ThreadState(threads.size(), nullptr, whichSamplingPlan);
}
