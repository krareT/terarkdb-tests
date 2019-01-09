//
// Created by frost on 18-9-20.
//

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstdlib>
#include <rocksdb/table.h>
#include "rocksdb.h"

RocksDB::RocksDB(Settings& settings_) :
        settings(settings_), db_name(settings.db_name)
{
  // rocksdb options
  db_options = settings.opt;

  fprintf(stderr, "Open db: %s\n", db_name.c_str());
  rocksdb::Status s = rocksdb::DB::Open(db_options, db_name, &db);
  if (!s.ok()) {
    fprintf(stderr, "FATAL: open db(%s) error: %s\n",
            db_name.c_str(), s.ToString().c_str());
    exit(1);
  }

  const auto& db_opt = db->GetOptions();

  fprintf(stderr,
  R"EOS(rocksdb options:
opt.write_buffer_size                   = %lld
opt.target_file_size_base               = %lld
opt.target_file_size_multiplier         = %d
opt.max_write_buffer_number             = %lld
opt.level0_file_num_compaction_trigger  = %lld
opt.level0_slowdown_writes_trigger      = %lld
opt.level0_stop_writes_trigger          = %lld
opt.soft_pending_compaction_bytes_limit = %lld
opt.hard_pending_compaction_bytes_limit = %lld
)EOS"
  , (long long)db_opt.write_buffer_size
  , (long long)db_opt.target_file_size_base
  , db_opt.target_file_size_multiplier
  , (long long)db_opt.max_write_buffer_number
  , (long long)db_opt.level0_file_num_compaction_trigger
  , (long long)db_opt.level0_slowdown_writes_trigger
  , (long long)db_opt.level0_stop_writes_trigger
  , (long long)db_opt.soft_pending_compaction_bytes_limit
  , (long long)db_opt.hard_pending_compaction_bytes_limit);

  if (!settings.file_path.empty()) {
    data_file = fopen(settings.file_path.c_str(), "r");
    if (data_file == nullptr) {
      fprintf(stderr, "FATAL: invalid filename: %s\n",
              settings.file_path.c_str());
      exit(1);
    }
  }
}

RocksDB::~RocksDB()
{
  assert(db != nullptr);
  delete db;
}

bool RocksDB::read_line(char* key, char* value)
{
  assert(data_file != nullptr);

  char* buf = nullptr;
  size_t len = 0;
  ssize_t read = getline(&buf, &len, data_file);
  if (read != -1) {
    ssize_t pos = 0;
    while (pos < read) {
      if (buf[pos++] == '\t') {
        memcpy(key, buf, (size_t)(pos - 1));
        key[pos - 1] = 0;

        if (value != nullptr) {
          memcpy(value, buf + pos, (size_t)(read - pos -1));
          value[read - pos -1] = 0;
        }
        break;
      }
    }
    free(buf);
  } else {
    fprintf(stderr, "Reach file EOF.\n");
    return false;
  }
  return true;
}

bool RocksDB::load()
{
  if (data_file == nullptr) {
    fprintf(stderr, "invalid filename: %s\n",
            settings.file_path.c_str());
    exit(-1);
  }
  printf("Begin load %ld lines data from %s...\n",
         settings.limit, settings.file_path.c_str());
  size_t nums = 0;
  auto key = (char*)malloc(20480 * sizeof(char));
  auto value = (char*)malloc(4096000 * sizeof(char));
  rocksdb::Status s;
  while (nums < settings.limit) {
    read_line(key, value);
    s = db->Put(write_options, key, "");
    // printf("key: %s value: %s\n", key, value);
    if (!s.ok()) {
      fprintf(stderr, "ERROR: error insert: %s && %s\n", key, value);
    }
    nums++;
    if (!(nums % 100000)) {
      printf("\rinsert: %ld", nums);
    }
  }

  printf("\n%s\n", "Load data done!");

  free(key);
  free(value);
  if (data_file != nullptr) {
    fclose(data_file);
    data_file = nullptr;
  }

  return true;
}

bool RocksDB::compact()
{
  rocksdb::CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  cro.target_level = 6;
  printf("%s\n", "Begin compact...");
  rocksdb::Status s = db->CompactRange(cro, nullptr, nullptr);
  if (!s.ok()) {
    fprintf(stderr, "Compact db error: %s\n", s.ToString().c_str());
    return false;
  }
  printf("%s\n", "Compact done!");
  return true;
}

void RocksDB::load_keys()
{
  if (data_file == nullptr) {
    fprintf(stderr, "invalid filename: %s\n",
            settings.file_path.c_str());
    exit(-1);
  }
  printf("Begin load %ld keys from %s...\n",
         settings.limit, settings.file_path.c_str());
  keys.resize(0);
  size_t nums = 0;
  auto key = (char *)malloc(20480 * sizeof(char));

  while (read_line(key, nullptr)) {
    keys.emplace_back(std::string(key));
    nums++;
  }

  printf("Read %lu keys.\n", nums);

  free(key);
  //if (data_file != nullptr) {
    fclose(data_file);
    data_file = nullptr;
    fprintf(stderr, "waaaaaahhhhhh, wen jian yi guan bi!\n");
  //}

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));
  if (settings.limit <= nums) {
    keys.resize(settings.limit);
    printf("Reserve %lu keys.\n", settings.limit);
  } else {
    printf("Settings.limit(%lu) < nums(%lu)\n", settings.limit, nums);
  }

  printf("%s\n", "Load keys done");
}

void RocksDB::verify()
{
  if (data_file == nullptr) {
    fprintf(stderr, "invalid filename: %s\n",
            settings.file_path.c_str());
    exit(-1);
  }
  printf("Begin load %ld keys - values from %s...\n",
         settings.limit, settings.file_path.c_str());
  keys.resize(0);
  size_t nums = 0;
  auto key = (char *)malloc(20480 * sizeof(char));
  auto value = (char *)malloc(4096000 * sizeof(char));

  while (nums < settings.limit) {
    read_line(key, value);
    keys_values.emplace_back(std::string(key), std::string(value));
    nums++;
  }

  free(key);
  free(value);
  fclose(data_file);
  data_file = nullptr;

  rocksdb::Iterator* iter = db->NewIterator(read_options);
  for (auto kv : keys_values) {
    iter->Seek(kv.first);
    if (iter->Valid()) {
      if (iter->value().ToString() != kv.second) {
        fprintf(stderr, "ERROR: key(%s) value don't match\n", kv.first.c_str());
      }
    } else {
      fprintf(stderr, "ERROR: seek key(%s) fail\n", kv.first.c_str());
    }
  }

  printf("verify done!\n");
}

void RocksDB::scan()
{
  rocksdb::Iterator* iter = db->NewIterator(read_options);
  iter->SeekToFirst();
  while(iter->Valid()) {
    iter->Next();
  }

  delete iter;
}

void RocksDB::scan_rev()
{
  rocksdb::Iterator* iter = db->NewIterator(read_options);
  iter->SeekToLast();
  while(iter->Valid()) {
    iter->Prev();
  }

  delete iter;
}

void RocksDB::seek()
{
  if (keys.empty()) {
    fprintf(stderr, "FATAL: please load keys first\n");
    exit(-1);
  }

  rocksdb::Iterator* iter = db->NewIterator(read_options);
  for (auto& key : keys) {
    iter->Seek(key);
  }

  delete iter;
}

void RocksDB::seek_prev(int64_t prev_times)
{
  if (keys.empty()) {
    fprintf(stderr, "FATAL: please load keys first\n");
    exit(-1);
  }

  rocksdb::Iterator* iter = db->NewIterator(read_options);
  for (auto& key : keys) {
    iter->Seek(key);
    if (!iter->Valid()) {
      fprintf(stderr, "Seek key=%s error\n", key.c_str());
    }
    for (int i = 0; i < prev_times; ++i) {
      iter->Prev();
      if (!iter->Valid()) {
        break;
      }
    }
  }

  delete iter;
}

void RocksDB::seek_next(int64_t next_times)
{
  if (keys.empty()) {
    fprintf(stderr, "FATAL: please load keys first\n");
    exit(-1);
  }

  rocksdb::Iterator* iter = db->NewIterator(read_options);
  for (auto& key : keys) {
    iter->Seek(key);
    if (!iter->Valid()) {
      fprintf(stderr, "Seek key=%s error\n", key.c_str());
    }
    for (int i = 0; i < next_times; ++i) {
      iter->Next();
      if (!iter->Valid()) {
        break;
      }
    }
  }

  delete iter;
}