/*
 * bench_terarkdb.cpp
 *
 *  Created on: 2017年3月30日
 *      Author: leipeng
 */

#include <src/TerichBenchmark.h>
#include "main_impl.h"

int main(int argc, char **argv) {
  return main_impl(argc, argv, "terichdb", NEW_BECHMARK(TerichBenchmark));
}
