cmake_minimum_required(VERSION 3.5)

set(CMAKE_C_COMPILER "clang")
set(TERARKDB_PATH ../../../terark-db-Darwin-x86_64-clang-7.3-bmi2-0/include)

project(TerarkDB_Test_Framework)
add_subdirectory(./src/engine)
