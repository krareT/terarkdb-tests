cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_COMPILER g++)
set(EXECUTABLE_OUTPUT_PATH ./build)

set(TERARKDB_PATH /Users/royguo/RoysData/terark/terark-db-Darwin-x86_64-clang-7.3-bmi2-0)

project(TerarkDB_Test_Framework)
add_subdirectory(./src/engine)