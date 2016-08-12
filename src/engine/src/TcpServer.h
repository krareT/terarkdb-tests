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
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/array.hpp>
#include "Setting.h"
#include <boost/algorithm/string.hpp>
#include <unordered_map>
#include "leveldb.h"
#include "src/Benchmark.h"
using boost::asio::ip::tcp;

class Session
        : public std::enable_shared_from_this<Session>
{
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {};

    void start(Setting *setting1,Benchmark *bm) {
        setting = setting1;
        benchmark = bm;
        do_read();
    }

private:
    void do_read();
    void read_line_handler(const boost::system::error_code& ec,std::size_t size);
    void do_write(std::string &message);

    tcp::socket socket_;
    boost::asio::streambuf buf_;
    Setting *setting;
    Benchmark *benchmark;
};
class Server
{
    Setting &setting;
    Benchmark *benchmark;
public:
    Server(boost::asio::io_service& io_service, short port, Setting &setting1,Benchmark *bm)
            : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
              socket_(io_service),
              setting(setting1),
              benchmark(bm)
    {
        do_accept();
    }

private:
    void do_accept()
    {
        acceptor_.async_accept(socket_,
                               [this](boost::system::error_code ec)
                               {
                                   if (!ec)
                                   {
                                       std::make_shared<Session>(std::move(socket_))->start(&setting,benchmark);
                                   }

                                   do_accept();
                               });
    }

    tcp::acceptor acceptor_;
    tcp::socket socket_;
};

#endif //TERARKDB_TEST_FRAMEWORK_TCPSERVER_H
