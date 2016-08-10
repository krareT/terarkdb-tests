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
#include "src/TerarkBenchmark.h"
using boost::asio::ip::tcp;

class Session
        : public std::enable_shared_from_this<Session>
{
public:
    Session(tcp::socket socket)
            : socket_(std::move(socket))
    {
    }

    void start(Setting *setting1,Benchmark *bm)
    {
        setting = setting1;
        benchmark = bm;
        do_read();
    }

private:
    void do_read()
    {
        auto self(shared_from_this());
        boost::asio::async_read_until(socket_, buf_, "\n",
                                      [this,self](boost::system::error_code ec, std::size_t lenth)
                                      {
                                          read_line_handler(ec,lenth);
                                          do_read();

                                      });
    }
    void read_line_handler(const boost::system::error_code& ec,std::size_t size) {

        if (ec)
            return;
        std::istream is(&buf_);
        std::string line;
        std::getline(is, line);
        std::cout << "Get:" << line << std::endl;
        std::string message;
        if (line == "query ops"){
            message = benchmark->GatherTimeData();
            //message = "Hi~\n\n\n";
        }else {
            message = setting->baseSetting.setBaseSetting(line);
        }
        message += "\nEND\r\n";
        do_write(message);
    }
    void do_write(std::string &message)
    {
        auto self(shared_from_this());

        boost::asio::async_write(socket_, boost::asio::buffer(message,message.size()),
                                 [this, self,message](boost::system::error_code ec, std::size_t transferred_byte)
                                 {
                                    if (transferred_byte == message.size())
                                        std::cout << "Reply finish!" << std::endl;
                                    else{
                                        std::cout << "Send:" << transferred_byte << std::endl;
                                        std::string msg = message.substr(transferred_byte);
                                        do_write(msg);
                                    }
                                 });
    }

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
