//
// Created by terark on 16-9-8.
//

#include "TerocksBenchmark.h"
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <table/terark_zip_table.h>
#include <fstream>

using namespace rocksdb;

TerocksBenchmark::TerocksBenchmark(Setting& set) : RocksDbBenchmark(set) {
    fprintf(stderr, "%s\n", BOOST_CURRENT_FUNCTION);
    rocksdb::TerarkZipTableOptions opt;
    if (set.terocksdb_tmpdir.empty()) {
        throw std::invalid_argument("argument --terocksdb_tmpdir=/some/dir is required !\n");
    }
//    if (!set.FLAGS_use_mmap) {
//        fprintf(stderr, "WARN: TerocksBenchmark: --use_mmap should be true\n");
//    }
    options.allow_mmap_reads = true; // force true
    opt.terarkZipMinLevel = set.terocksdbZipMinLevel;
    opt.localTempDir = set.terocksdb_tmpdir;
    opt.sampleRatio = set.terocksdbSampleRatio;
    opt.checksumLevel = set.checksumLevel;
    opt.indexNestLevel = std::min(9, std::max(2, set.terocksdbIndexNestLevel));
    opt.indexCacheRatio = set.terocksdbIndexCacheRatio;
    opt.softZipWorkingMemLimit = set.terocksdbZipWorkingMemSoftLimit;
    opt.hardZipWorkingMemLimit = set.terocksdbZipWorkingMemHardLimit;
    opt.smallTaskMemory = set.terocksdbSmallTaskMemory;
    if (set.terocksdbCacheShards > 0) {
        opt.cacheShards = set.terocksdbCacheShards;
        opt.cacheCapacityBytes = set.FLAGS_cache_size;
        opt.minPreadLen = set.terocksdbMinPreadLen;
        fprintf(stderr, "cacheShards: %d, cacheCapacityBytes: %lu\n", opt.cacheShards, opt.cacheCapacityBytes);
    }
#if 1
    rocksdb::TableFactory *factory = NewTerarkZipTableFactory(opt, NewBlockBasedTableFactory());
#else
//    options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(2)); // fuck fuck
//    rocksdb::PlainTableOptions pto;
//    pto.store_index_in_file = true; // fuck
//    pto.hash_table_ratio = 0.0; // fuck
//    pto.encoding_type = rocksdb::kPlain;
//    rocksdb::TableFactory *factory = NewTerarkZipTableFactory(opt, rocksdb::NewPlainTableFactory(pto));
#endif
    options.table_factory.reset(factory);

    options.target_file_size_base = 512 << 20; // 512M
    options.target_file_size_multiplier = set.target_file_size_multiplier;

    // for TerarkZipTable, set to 1 will yield larger SST file.
    // for rocksdb's official SST,
    // we set max_subcompactions = 2, see RocksDbBenchmark::RocksDbBenchmark()
    options.max_subcompactions = 1;

    setting.dbdirs.push_back(opt.localTempDir);
}

std::string TerocksBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static const std::unordered_map<std::string,
            std::pair<bool (TerocksBenchmark::*)(const std::string &),
                      std::string (TerocksBenchmark::*)(void)
                     >
    > handleFuncMap = {
		{ "terark_zip_min_level", { &TerocksBenchmark::setTerarkZipMinLevel,
									&TerocksBenchmark::getTerarkZipMinLevel }},
		{ "check_sum_level",  { &TerocksBenchmark::setCheckSumLevel,
								&TerocksBenchmark::getCheckSumLevel }},
		{ "sampling_rate", { &TerocksBenchmark::setSamplingRatio,
							 &TerocksBenchmark::getSamplingRatio }},
		{ "index_nest_level", { &TerocksBenchmark::setIndexNestLevel,
								&TerocksBenchmark::getIndexNestLevel }},
		{ "estimate_compression_ratio", { &TerocksBenchmark::setEstimateCompressionRatio,
										  &TerocksBenchmark::getEstimateCompressionRatio }},
    };
    size_t div = msg.find(':');
    if (div == std::string::npos) {
        fprintf(stderr, "ERROR: TerarkRocksDbBenchmark::HandleMessage: msg = %s\n", msg.c_str());
        return ss.str();
    }
    std::string key = msg.substr(0, div);
    std::string value = msg.substr(div + 1);
    auto iter = handleFuncMap.find(key);
    if (handleFuncMap.end() != iter) {
	    if ((this->*(iter->second.first))(value)){
	    	ss << "set " << key << ": success" << std::endl;
	    }else{
	    	ss << "set " << key << ": failed" << std::endl;
	    }
    }else{
    	ss << "invalid cmd!" << std::endl;
    }
    for (auto each : handleFuncMap) {
        ss << each.first << ":" << (this->*each.second.second)() << std::endl;
    }
    puts(ss.str().c_str());
    return ss.str();
}

bool TerocksBenchmark::setTerarkZipMinLevel(const std::string &val) {
    int value = std::stoi(val);
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->terarkZipMinLevel = value;
    return true;
}

std::string TerocksBenchmark::getTerarkZipMinLevel(void) {
    int value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->terarkZipMinLevel;
    std::stringstream ss;
    ss << value;
    return ss.str();
}

std::string TerocksBenchmark::getCheckSumLevel(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->checksumLevel;
    ss << value;
    return ss.str();
}

bool TerocksBenchmark::setCheckSumLevel(const std::string &val) {
    auto value = std::stoi(val);
    if (value > 2 || value < 0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->checksumLevel = value;
    return true;
}

bool TerocksBenchmark::setSamplingRatio(const std::string &val) {
    char *end;
    double value = strtod(val.c_str(), &end);
    if (value <= 0.0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->sampleRatio = value;
    return true;
}

std::string TerocksBenchmark::getSamplingRatio(void) {
    std::stringstream ss;
    ss << reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->sampleRatio;
    return ss.str();
}

bool TerocksBenchmark::setIndexNestLevel(const std::string &val) {
    int value = std::stoi(val);
    if (value < 0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->indexNestLevel = value;
    return true;
}

std::string TerocksBenchmark::getIndexNestLevel(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->indexNestLevel;
    ss << value;
    return ss.str();
}

bool TerocksBenchmark::setEstimateCompressionRatio(const std::string &val) {
    char *end;
    float value = strtof(val.c_str(),&end);
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->estimateCompressionRatio = value;
    return true;
}

std::string TerocksBenchmark::getEstimateCompressionRatio(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->estimateCompressionRatio;
    ss << value;
    return ss.str();
}
