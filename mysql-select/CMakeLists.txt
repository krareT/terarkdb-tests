cmake_minimum_required(VERSION 3.9)
project(mysql_select)

set(CMAKE_CXX_STANDARD 11)

include_directories(/usr/include/mysql)

link_directories(/usr/lib/x86_64-linux-gnu)

add_executable(select main.cpp)

target_link_libraries(select mysqlclient pthread z m rt dl)