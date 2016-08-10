cmake_minimum_required(VERSION 3.5)
project(sync_tcp_echo_client)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(sync_tcp_echo_client ${SOURCE_FILES})
target_link_libraries(sync_tcp_echo_client boost_system pthread)