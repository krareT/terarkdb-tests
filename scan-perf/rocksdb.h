//
// Created by frost on 18-9-20.
//

#ifndef SCAN_PERF_ROCKSDB_H
#define SCAN_PERF_ROCKSDB_H

#include <string>
#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

class RocksDB {
public:
  explicit RocksDB(std::string&& db_name);
  ~RocksDB();
  bool load(std::string fpath, size_t limit);
  bool compact();
  void run();

private:
  std::string db_name;
  rocksdb::DB* db;
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;
  rocksdb::Options db_options;
  std::vector<std::string> keys;

  void load_key(std::string fpath, size_t limit);
};


#endif //SCAN_PERF_ROCKSDB_H
