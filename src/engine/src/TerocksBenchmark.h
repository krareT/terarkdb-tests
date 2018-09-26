//
// Created by terark on 16-9-8.
//

#ifndef TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H

#include "RocksDbBenchmark.h"
class TerocksBenchmark : public RocksDbBenchmark {
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

    void PrintTerarkdbCacheState();
public:
    TerocksBenchmark(Setting& set);
    void Close() override;
    void PrintCacheState() override ;
};


#endif //TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
