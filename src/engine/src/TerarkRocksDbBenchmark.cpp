//
// Created by terark on 16-9-8.
//

#include "TerarkRocksDbBenchmark.h"
#include <rocksdb/slice_transform.h>

TerarkRocksDbBenchmark::TerarkRocksDbBenchmark(Setting &set) : RocksDbBenchmark(set) {
    std::cout <<"TR Benchmark" << std::endl;
    rocksdb::TerarkZipTableOptions opt;
    if (set.terocksdb_tmpdir.empty()) {
        throw std::invalid_argument("argument --terocksdb_tmpdir=/some/dir is required !\n");
    }
    opt.terarkZipMinLevel = 0;
    opt.localTempDir = set.terocksdb_tmpdir;
    opt.sampleRatio = 0.015;
    opt.indexNestLevel = std::min(9, std::max(2, set.terocksdbIndexNestLevel));
    opt.indexCacheRatio = set.terocksdbIndexCacheRatio;
    opt.softZipWorkingMemLimit = 16ull<<30;
    opt.hardZipWorkingMemLimit = 28ull<<30;
//  printf("local temp dir:%s\n",tmp_dir);
#if 1
    rocksdb::TableFactory *factory = NewTerarkZipTableFactory(opt, rocksdb::NewBlockBasedTableFactory());
#else
//    options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(2)); // fuck fuck
//    rocksdb::PlainTableOptions pto;
//    pto.store_index_in_file = true; // fuck
//    pto.hash_table_ratio = 0.0; // fuck
//    pto.encoding_type = rocksdb::kPlain;
//    rocksdb::TableFactory *factory = NewTerarkZipTableFactory(opt, rocksdb::NewPlainTableFactory(pto));
#endif
    options.table_factory.reset(factory);
    // rocksdb preallocate space for SST file aggressively
    // so do not make target_file_size too large
    options.target_file_size_base = 64 << 20; // 64M
    options.target_file_size_multiplier = set.target_file_size_multiplier;

    // for TerarkZipTable, set to 1 will yield larger SST file.
    // for rocksdb's official SST,
    // we set max_subcompactions = 2, see RocksDbBenchmark::RocksDbBenchmark()
    options.max_subcompactions = 1;

    setting.dbdirs.push_back(opt.localTempDir);
}

std::string TerarkRocksDbBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static const std::unordered_map<std::string,
            std::pair<bool (TerarkRocksDbBenchmark::*)(const std::string &),
                      std::string (TerarkRocksDbBenchmark::*)(void)
                     >
    > handleFuncMap = {
		{ "terark_zip_min_level", { &TerarkRocksDbBenchmark::setTerarkZipMinLevel,
									&TerarkRocksDbBenchmark::getTerarkZipMinLevel }},
		{ "check_sum_level",  { &TerarkRocksDbBenchmark::setCheckSumLevel,
								&TerarkRocksDbBenchmark::getCheckSumLevel }},
		{ "sampling_rate", { &TerarkRocksDbBenchmark::setSamplingRatio,
							 &TerarkRocksDbBenchmark::getSamplingRatio }},
		{ "index_nest_level", { &TerarkRocksDbBenchmark::setIndexNestLevel,
								&TerarkRocksDbBenchmark::getIndexNestLevel }},
		{ "estimate_compression_ratio", { &TerarkRocksDbBenchmark::setEstimateCompressionRatio,
										  &TerarkRocksDbBenchmark::getEstimateCompressionRatio }},
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

bool TerarkRocksDbBenchmark::setTerarkZipMinLevel(const std::string &val) {
    int value = std::stoi(val);
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->terarkZipMinLevel = value;
    return true;
}

std::string TerarkRocksDbBenchmark::getTerarkZipMinLevel(void) {
    int value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->terarkZipMinLevel;
    std::stringstream ss;
    ss << value;
    return ss.str();
}

std::string TerarkRocksDbBenchmark::getCheckSumLevel(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->checksumLevel;
    ss << value;
    return ss.str();
}

bool TerarkRocksDbBenchmark::setCheckSumLevel(const std::string &val) {
    auto value = std::stoi(val);
    if (value > 2 || value < 0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->checksumLevel = value;
    return true;
}

bool TerarkRocksDbBenchmark::setSamplingRatio(const std::string &val) {
    char *end;
    double value = strtod(val.c_str(), &end);
    if (value <= 0.0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->sampleRatio = value;
    return true;
}

std::string TerarkRocksDbBenchmark::getSamplingRatio(void) {
    std::stringstream ss;
    ss << reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->sampleRatio;
    return ss.str();
}

bool TerarkRocksDbBenchmark::setIndexNestLevel(const std::string &val) {
    int value = std::stoi(val);
    if (value < 0)
        return false;
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->indexNestLevel = value;
    return true;
}

std::string TerarkRocksDbBenchmark::getIndexNestLevel(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->indexNestLevel;
    ss << value;
    return ss.str();
}

bool TerarkRocksDbBenchmark::setEstimateCompressionRatio(const std::string &val) {
    char *end;
    float value = strtof(val.c_str(),&end);
    reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->estimateCompressionRatio = value;
    return true;
}

std::string TerarkRocksDbBenchmark::getEstimateCompressionRatio(void) {
    std::stringstream ss;
    auto value = reinterpret_cast<rocksdb::TerarkZipTableOptions*>(options.table_factory->GetOptions())->estimateCompressionRatio;
    ss << value;
    return ss.str();
}
