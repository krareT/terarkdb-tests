//
// Created by terark on 16-9-8.
//

#ifndef TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H

#include "RocksDbBenchmark.h"
#include <table/terark_zip_table.h>
class TerarkRocksDbBenchmark : public RocksDbBenchmark{
public:
    TerarkRocksDbBenchmark(Setting &set) : RocksDbBenchmark(set) {
        std::cout <<"TR Benchmark" << std::endl;
        rocksdb::TerarkZipTableOptions opt;
        if ( opt.localTempDir != "/tmp")
            opt.localTempDir = "/tmp";
        rocksdb::TableFactory* factory = NewTerarkZipTableFactory(opt);
        options.table_factory.reset(factory);
    }

};


#endif //TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
