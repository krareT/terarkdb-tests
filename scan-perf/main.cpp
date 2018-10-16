#include <iostream>
#include <benchmark/benchmark.h>
#include <rocksdb/table.h>

#include "rocksdb.h"

using namespace rocksdb;

auto scan = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->scan();
  }
};

auto scan_rev = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->scan_rev();
  }
};

auto seek = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->seek();
  }
};

auto seek_next = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->seek_next(state.range(0));
  }
};

auto seek_prev = [](benchmark::State& state, RocksDB* db) {
  for (auto _ : state) {
    db->seek_prev(state.range(0));
  }
};

int main(int argc, char* argv[])
{
  Settings settings;
  settings.db_name = "testdb";

  // rocksdb options
  settings.opt.create_if_missing = true;
  rocksdb::BlockBasedTableOptions bbto;
  bbto.block_cache = rocksdb::NewLRUCache(32ULL << 30, 8, false);
  bbto.block_size = 16 << 10;
  settings.opt.compression = CompressionType::kSnappyCompression;
  settings.opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

  if (argc >= 3) {
    settings.file_path = argv[2];
  }
  if (argc >= 4) {
    settings.limit = (size_t)atoi(argv[3]);
  }

  RocksDB* db = new RocksDB(settings);

  if (argc < 2) {
    goto usage;
  }

  if (!strcmp(argv[1], "load")) {
    db->load();
    //db->compact();
  } else if (!strcmp(argv[1], "compact")) {
    db->compact();
  } else if (!strcmp(argv[1], "run")) {
    //db->load_keys();
    printf("%s\n", "benchmark begin!");

    benchmark::RegisterBenchmark("warn-up", scan, db)->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark("scan", scan, db)->Unit(benchmark::kMillisecond)->Iterations(100);
    benchmark::RegisterBenchmark("scan_rev", scan_rev, db)->Unit(benchmark::kMillisecond);
    //benchmark::RegisterBenchmark("seek", seek, db)->Unit(benchmark::kMillisecond);
    //benchmark::RegisterBenchmark("seek_next", seek_next, db)->Unit(benchmark::kMillisecond)->Arg(1);
    //benchmark::RegisterBenchmark("seek_prev", seek_prev, db)->Unit(benchmark::kMillisecond)->Arg(1);
    //benchmark::RegisterBenchmark("seek_next", seek_next, db)->Unit(benchmark::kMillisecond)->Arg(10);
    //benchmark::RegisterBenchmark("seek_prev", seek_prev, db)->Unit(benchmark::kMillisecond)->Arg(10);
    //benchmark::RegisterBenchmark("seek_next", seek_next, db)->Unit(benchmark::kMillisecond)->Arg(100);
    //benchmark::RegisterBenchmark("seek_prev", seek_prev, db)->Unit(benchmark::kMillisecond)->Arg(100);
    benchmark::RunSpecifiedBenchmarks();
  } else if (!strcmp(argv[1], "verify")) {
    db->verify();
  } else {
    goto usage;
  }

  delete db;

  return 0;

  usage:
  fprintf(stderr, "%s\n", "Usage: ./scan_perf {load [file_path] [nums]}/{run [file_path] [nums]}/compact");
  return 1;
}