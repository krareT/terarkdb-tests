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

using boost::asio::ip::tcp;

class tcp_connection : public boost::enable_shared_from_this<tcp_connection>{
public:
    typedef boost::shared_ptr<tcp_connection> pointer;
    static pointer create(boost::asio::io_service &io_service){
        return pointer(new tcp_connection(io_service));
    }
    tcp::socket& socket(){
        return socket_;
    }
    void start(Setting &setting){
        message_ = std::string("Hi!I am Terark_Engine_Test!\n");
        boost::system::error_code error;
        boost::array<char,128> buf;
        size_t len = socket_.read_some(boost::asio::buffer(buf),error);

        if (setting.baseSetting.getReadPercent() == 100)
            setting.baseSetting.setReadPercent(0);
        else
            setting.baseSetting.setReadPercent(100);
        std::cout << "\nREAD_PERCENT:" << (int) (setting.baseSetting.getReadPercent()) << std::endl;
        boost::asio::async_write(socket_, boost::asio::buffer(message_),
                                 boost::bind(&tcp_connection::handle_write, shared_from_this(),
                                             boost::asio::placeholders::error,
                                             boost::asio::placeholders::bytes_transferred));
    };
private:
    tcp_connection(boost::asio::io_service& io_service)
            : socket_(io_service)
    {
    }

    void handle_write(const boost::system::error_code& /*error*/,
                      size_t /*bytes_transferred*/)
    {

    }

    tcp::socket socket_;
    std::string message_;
};
class tcp_server{
private:
    tcp::acceptor acceptor_;
    Setting &setting;

public:
    tcp_server(boost::asio::io_service& io_service,Setting &setting1)
            :   acceptor_(io_service, tcp::endpoint(tcp::v4(),13)),
                setting(setting1){
        start_accept();
    }
private:
    void start_accept(){
        tcp_connection::pointer newConnection = tcp_connection::create(acceptor_.get_io_service());
        acceptor_.async_accept(newConnection->socket(),
                               boost::bind(&tcp_server::handle_accept, this, newConnection,
                                           boost::asio::placeholders::error));
    }
    void handle_accept(tcp_connection::pointer new_conn, const boost::system::error_code& error){
        if (!error){
            new_conn->start(setting);
        }
        start_accept();
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_TCPSERVER_H
