//
// Created by terark on 16-8-2.
//

#ifndef TERARKDB_TEST_FRAMEWORK_TCPSERVER_H
#define TERARKDB_TEST_FRAMEWORK_TCPSERVER_H

#pragma once
#include <ctime>
#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/algorithm/string.hpp>
#include <unordered_map>
#include "Benchmark.h"
#include "Setting.h"

using boost::asio::ip::tcp;

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket);
    void start(Setting *setting1,Benchmark *bm);
private:
    void do_read();
    void read_line_handler(const boost::system::error_code& ec,std::size_t size);
    void do_write(std::string &message);
private:
    tcp::socket socket_;
    boost::asio::streambuf buf_;
    Setting *setting;
    Benchmark *benchmark;
};
class Server {
    tcp::acceptor acceptor_;
    tcp::socket socket_;
    Setting &setting;
    Benchmark *benchmark;
public:
    Server(boost::asio::io_service&, short port, Setting&, Benchmark*);
private:
    void do_accept();
};

#endif //TERARKDB_TEST_FRAMEWORK_TCPSERVER_H
