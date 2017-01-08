//
// Created by terark on 16-9-8.
//

#ifndef TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H

#include "RocksDbBenchmark.h"
#include <table/terark_zip_table.h>
class TerarkRocksDbBenchmark : public RocksDbBenchmark{
private:
    virtual std::string HandleMessage(const std::string &msg) override;
    bool setTerarkZipMinLevel(const std::string &);
    std::string getTerarkZipMinLevel(void);
    bool setCheckSumLevel(const std::string&);
    std::string getCheckSumLevel(void);
    bool setSamplingRatio(const std::string &);
    std::string getSamplingRatio(void);
    bool setIndexNestLevel(const std::string &);
    std::string getIndexNestLevel(void);
    bool setEstimateCompressionRatio(const std::string &);
    std::string getEstimateCompressionRatio(void);
public:
    TerarkRocksDbBenchmark(Setting &set);
};


#endif //TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
