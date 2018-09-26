//
// Created by frost on 18-9-20.
//

#include <benchmark/benchmark.h>
#include "rocksdb.h"

RocksDB::RocksDB(std::string&& db_name_) : db_name(std::move(db_name_))
{
  // rocksdb options
  db_options.create_if_missing = true;

  fprintf(stderr, "Open db: %s\n", db_name.c_str());
  rocksdb::Status s = rocksdb::DB::Open(db_options, db_name, &db);
  if (!s.ok()) {
    fprintf(stderr, "FATAL: db(%s) open error: %s\n",
            db_name.c_str(), s.ToString().c_str());
    exit(1);
  }

  const auto& opt = db->GetOptions();

  fprintf(stderr,
  R"EOS(rocksdb options:
opt.write_buffer_size              = %lld
opt.target_file_size_base          = %lld
opt.target_file_size_multiplier    = %d
)EOS"
  , (long long)opt.write_buffer_size
  , (long long)opt.target_file_size_base
  , opt.target_file_size_multiplier);
}

RocksDB::~RocksDB()
{
  assert(db != nullptr);
  delete db;
}

bool RocksDB::load(std::string fpath, size_t limit)
{
  FILE* file = fopen(fpath.c_str(), "r");
  if (file == nullptr) {
    fprintf(stderr, "FATAL: invalid filename: %s\n", fpath.c_str());
    exit(1);
  }

  printf("Begin load %ld lines data from %s\n", limit, fpath.c_str());
  size_t nums = 0;
  char* buf = (char *)malloc(2048000 * sizeof(char));
  size_t len;
  auto key = (char *)malloc(10240 * sizeof(char));
  auto value = (char *)malloc(2048000 * sizeof(char));
  rocksdb::Status s;
  while (nums < limit) {
    ssize_t read = getline(&buf, &len, file);
    if (read != -1) {
      ssize_t pos = 0;
      while (pos < read) {
        if (buf[pos++] == '\t') {
          memcpy(key, buf, (size_t)(pos - 1));
          key[pos - 1] = 0;
          memcpy(value, buf + pos, (size_t)(read - pos -1));
          value[read - pos -1] = 0;

          s = db->Put(write_options, key, value);
          // printf("key: %s value: %s\n", key, value);
          if (!s.ok()) {
            fprintf(stderr, "WARN: error insert: %s\n", buf);
          }

          break;
        }
      }
    } else {
      fprintf(stderr, "ERROR: file EOF, lines < limit(%ld).\n", limit);
      break;
    }
    nums++;
    if (!(nums % 1000)) {
      printf("\rinsert: %ld", nums);
    }
  }

  printf("\n%s\n", "Load data done!");

  free(buf);
  free(key);
  free(value);

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

void RocksDB::load_key(std::string fpath, size_t limit)
{
  FILE* file = fopen(fpath.c_str(), "r");
  if (file == nullptr) {
    fprintf(stderr, "FATAL: invalid filename: %s\n", fpath.c_str());
    exit(1);
  }

  printf("Begin load %ld keys from %s\n", limit, fpath.c_str());
  size_t nums = 0;
  char* buf = (char *)malloc(2048000 * sizeof(char));
  size_t len;
  auto key = (char *)malloc(10240 * sizeof(char));

  while (nums < limit) {
    ssize_t read = getline(&buf, &len, file);
    if (read != -1) {
      ssize_t pos = 0;
      while (pos < read) {
        if (buf[pos++] == '\t') {
          memcpy(key, buf, (size_t) (pos - 1));
          key[pos - 1] = 0;
          keys.emplace_back(std::string(key));

          break;
        }
      }
    } else {
      fprintf(stderr, "ERROR: file EOF, lines < limit(%ld).\n", limit);
      break;
    }
  }

  printf("%s\n", "Load keys done");
}

void RocksDB::run()
{
  load_key("data.txt", 10000);
  rocksdb::Iterator* iter = db->NewIterator(read_options);
  for(auto& key : keys) {
    iter->Seek(key);
    if (!iter->Valid()) {
      fprintf(stderr, "Seek key=%s error\n", key.c_str());
    }
    for (int i = 0; i < 10; i++) {
      iter->Prev();
      if (!iter->Valid()) {
        break;
      }
    }
  }
}