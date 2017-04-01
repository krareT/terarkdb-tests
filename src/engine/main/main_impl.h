#pragma once

class Benchmark;
class Setting;

int main_impl(int argc, char **argv
            , const char* default_engine_name
            , Benchmark* (*newBenchmark)(Setting&)
			);

#define NEW_BECHMARK(Class) \
  [](Setting& s){return static_cast<Benchmark*>(new Class(s));}
