//
// Created by frost on 18-9-20.
//

#ifndef SCAN_PERF_ROCKSDB_H
#define SCAN_PERF_ROCKSDB_H

#include <string>
#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

struct Settings {
  std::string db_name = "testdb";
  std::string file_path;
  size_t limit = 100;

  rocksdb::Options opt;
};

class RocksDB {
public:
  explicit RocksDB(Settings& settings);
  ~RocksDB();
  bool load();
  bool compact();
  void load_keys();
  void scan();
  void scan_rev();
  void seek();
  void seek_prev(int64_t prev_times);
  void seek_next(int64_t next_times);
  void verify();

private:
  Settings settings;

  std::string db_name;
  rocksdb::DB* db;
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;
  rocksdb::Options db_options;

  FILE* data_file;
  std::vector<std::string> keys;
  std::vector<std::pair<std::string, std::string>> keys_values;

  bool read_line(char* key, char* value);
};


#endif //SCAN_PERF_ROCKSDB_H
