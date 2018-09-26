#include <iostream>
#include <benchmark/benchmark.h>
#include <unistd.h>

#include "rocksdb.h"

using namespace rocksdb;

/*
static void test(benchmark::State& state, RocksDB* db)
{
  for (auto _ : state) {
    printf("%s\n", "once");
    db->run();
  }
}
*/

auto test = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->run();
  }
};

int main(int argc, char* argv[])
{
  RocksDB* db = new RocksDB("testdb");

  if (argc < 2) {
    goto usage;
  }
  if (!strcmp(argv[1], "load")) {
    std::string file_path = "data.txt";
    size_t nums = 1000;

    if (argc >= 3) {
      file_path = argv[2];
    }
    if (argc >= 4) {
      nums = (size_t)atoi(argv[3]);
    }

    db->load(file_path, nums);
    db->compact();
  } else if (!strcmp(argv[1], "compact")) {
    db->compact();
  } else if (!strcmp(argv[1], "run")) {
    printf("%s\n", "benchmark begin!");

    benchmark::RegisterBenchmark("test", test, db);
    benchmark::RunSpecifiedBenchmarks();
  } else {
    goto usage;
  }

  delete db;

  return 0;

  usage:
  fprintf(stderr, "%s\n", "Usage: ./scan_perf {load [file path] [nums]}/compact/run");
  return 1;
}