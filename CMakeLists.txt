cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_COMPILER g++)
set(EXECUTABLE_OUTPUT_PATH ./build)

set(TERARKDB_PATH ./)

project(TerarkDB_Test_Framework)
add_subdirectory(./src/engine)