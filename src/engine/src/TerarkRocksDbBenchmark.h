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
        char *tmp_dir = getenv("TerRocksdb_Tmpdir");
	if ( tmp_dir == NULL || strlen(tmp_dir) == 0){
		throw std::invalid_argument("U must set the tmp_dir!\n");
	}
        opt.localTempDir = tmp_dir;
       	printf("local temp dir:%s\n",tmp_dir);
       	rocksdb::TableFactory* factory = NewTerarkZipTableFactory(opt);
        options.table_factory.reset(factory);
    }

};


#endif //TERARKDB_TEST_FRAMEWORK_TERARKROCKSDBBENCHMARK_H
