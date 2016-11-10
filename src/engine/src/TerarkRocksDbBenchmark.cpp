//
// Created by terark on 16-9-8.
//

#include "TerarkRocksDbBenchmark.h"

std::string TerarkRocksDbBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static std::unordered_map<std::string, std::pair<bool (TerarkRocksDbBenchmark::*)(const std::string &),
            std::string (TerarkRocksDbBenchmark::*)(void)>> handleFuncMap;
    handleFuncMap["terark_zip_min_level"] = std::make_pair(&TerarkRocksDbBenchmark::setTerarkZipMinLevel,
                                                     &TerarkRocksDbBenchmark::getTerarkZipMinLevel);
    handleFuncMap["check_sum_level"] = std::make_pair(&TerarkRocksDbBenchmark::setCheckSumLevel,
                                                    &TerarkRocksDbBenchmark::getCheckSumLevel);
    handleFuncMap["sampling_rate"] = std::make_pair(&TerarkRocksDbBenchmark::setSamplingRatio,
                                                    &TerarkRocksDbBenchmark::getSamplingRatio);
    handleFuncMap["index_nest_level"] = std::make_pair(&TerarkRocksDbBenchmark::setIndexNestLevel,
                                                       &TerarkRocksDbBenchmark::getIndexNestLevel);
    handleFuncMap["estimate_compression_ratio"] = std::make_pair(&TerarkRocksDbBenchmark::setEstimateCompressionRatio,
                                                                 &TerarkRocksDbBenchmark::getEstimateCompressionRatio);
    size_t div = msg.find(':');
    if (div == std::string::npos)
        return ss.str();
    std::string key = msg.substr(0, div);
    std::string value = msg.substr(div + 1);
    if (handleFuncMap.count(key) > 0) {
	    if ((this->*(handleFuncMap[key].first))(value)){
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
