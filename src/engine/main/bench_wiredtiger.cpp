/*
 * bench_terarkdb.cpp
 *
 *  Created on: 2017年3月30日
 *      Author: leipeng
 */

#include <src/WiredTigerBenchmark.h>
#include "main_impl.h"

int main(int argc, char **argv) {
  return main_impl(argc, argv, "wiredtiger", NEW_BECHMARK(WiredTigerBenchmark));
}
