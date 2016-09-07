//
// Created by terark on 9/5/16.
//

#include "RocksDbBenchmark.h"
#include <string>
void RocksDbBenchmark::Open() {

    std::cout << "Create database " << setting.FLAGS_db << std::endl;
    rocksdb::Status s = rocksdb::DB::Open(options, setting.FLAGS_db, &db);
    if (!s.ok()) {
        fprintf(stderr, "open error: %s\n", s.ToString().c_str());
        exit(1);
    }

}

RocksDbBenchmark::RocksDbBenchmark(const Setting &set) : Benchmark(set) {

    db = nullptr;
    options.create_if_missing = true;
// new features to add

    options.allow_concurrent_memtable_write = true;
    options.enable_write_thread_adaptive_yield = true;
    options.allow_mmap_reads = true;
    options.allow_mmap_writes = true;
    options.max_background_compactions = 2;
// end
//
    rocksdb::BlockBasedTableOptions block_based_options;
    block_based_options.index_type = rocksdb::BlockBasedTableOptions::kBinarySearch;
    block_based_options.block_cache = setting.FLAGS_cache_size >= 0 ?
                                      rocksdb::NewLRUCache(setting.FLAGS_cache_size) : NULL;
    block_based_options.block_size = set.FLAGS_block_size;

    filter_policy_.reset(set.FLAGS_bloom_bits >= 0 ?
                         rocksdb::NewBloomFilterPolicy(set.FLAGS_bloom_bits, false) : NULL);
    block_based_options.filter_policy = filter_policy_;
    options.table_factory.reset(
            NewBlockBasedTableFactory(block_based_options));
    write_options = rocksdb::WriteOptions();
    read_options = rocksdb::ReadOptions();

    options.compression = set.FLAGS_compression_type;
    if (set.FLAGS_min_level_to_compress >= 0) {
        assert(set.FLAGS_min_level_to_compress <= set.FLAGS_num_levels);
        options.compression_per_level.resize(set.FLAGS_num_levels);
        for (int i = 0; i < set.FLAGS_min_level_to_compress; i++) {
            options.compression_per_level[i] = rocksdb::kNoCompression;
        }
        for (int i = set.FLAGS_min_level_to_compress;
             i < set.FLAGS_num_levels; i++) {
            options.compression_per_level[i] = set.FLAGS_compression_type;
        }
    }

}

void RocksDbBenchmark::Close() {
    delete db;
}

void RocksDbBenchmark::Load() {

    FILE *loadFile = fopen(setting.getLoadDataPath().c_str(), "r");
    assert(loadFile != NULL);
    posix_fadvise(fileno(loadFile), 0, 0, POSIX_FADV_SEQUENTIAL);
    char *buf;
    size_t n = 0;
    buf = NULL;
    std::string key;
    std::string value;
    std::string str;
    rocksdb::Status s;
	uint32_t lines_num = 0;
    while (-1 != getline(&buf, &n, loadFile)) {
        str = buf;
        if (n > 1024 * 1024) {
            free(buf);
            buf = NULL;
            n = 0;
        }
        auto ret = getKeyAndValue(str, key, value);
        if (ret == 0)
            continue;
        s = db->Put(write_options, key, value);
        if (!s.ok()) {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        }
        pushKey(key);
	lines_num++;
	if (lines_num % 10000 == 0)
		printf("load:%uw\n",lines_num/10000);
    }

}

size_t RocksDbBenchmark::getKeyAndValue(std::string &str, std::string &key, std::string &val) {
    std::vector<std::string> strvec;
    boost::split(strvec, str, boost::is_any_of("\t"));
    key.clear();
    val.clear();
    assert(key.size() + val.size() == 0);
    if (strvec.size() < 8)
        return 0;
    key = strvec[2] + strvec[7];
    for (int i = 0; i < strvec.size(); i++) {
        if (i == 2 || i == 7)
            continue;
        val.append(strvec[i]);
    }
    val = str;
    return strvec.size();
}

bool RocksDbBenchmark::ReadOneKey(ThreadState *ts) {

    if (false == getRandomKey(ts->key, ts->randGenerator))
        return false;
    if (false == db->Get(read_options, ts->key, &(ts->value)).ok())
        return false;
    return true;
}

bool RocksDbBenchmark::UpdateOneKey(ThreadState *ts) {

    if (false == getRandomKey(ts->key, ts->randGenerator))
        return false;
    if (false == db->Get(read_options, ts->key, &(ts->value)).ok())
        return false;
    if (false == db->Put(write_options, ts->key, ts->value).ok())
        return false;
    return true;
}

bool RocksDbBenchmark::InsertOneKey(ThreadState *ts) {

    if (updateDataCq.try_pop(ts->str) == false)
        return false;
    auto ret = getKeyAndValue(ts->str, ts->key, ts->value);
    if (ret == 0)
        return false;
    if (false == db->Put(write_options, ts->key, ts->value).ok())
        return false;
    pushKey(ts->key);
    return true;
}

bool RocksDbBenchmark::Compact(void) {
    return false;
}

ThreadState *RocksDbBenchmark::newThreadState(std::atomic<std::vector<uint8_t> *> *whichExecutePlan,
                                              std::atomic<std::vector<uint8_t> *> *whichSamplingPlan) {

    return new ThreadState(threads.size(), whichExecutePlan, whichSamplingPlan);
}
