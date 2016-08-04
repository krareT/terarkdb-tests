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

using boost::asio::ip::tcp;

class tcp_connection : public boost::enable_shared_from_this<tcp_connection>{
public:
    typedef boost::shared_ptr<tcp_connection> pointer;
    static pointer create(boost::asio::io_service &io_service,Setting &setting){
        return pointer(new tcp_connection(io_service,setting));
    }
    tcp::socket& socket(){
        return socket_;
    }
    void start(Setting &setting){
        printf("Handle Accept!\n");
        boost::system::error_code error;

        boost::asio::async_read_until(socket_,data_,"\n",
            boost::bind(&tcp_connection::handle_read, this, _1));
        message_.clear();

        boost::asio::async_write(socket_, boost::asio::buffer(message_),
                                 boost::bind(&tcp_connection::handle_write, shared_from_this(),
                                             boost::asio::placeholders::error,
                                             boost::asio::placeholders::bytes_transferred));

    };
    tcp_connection(boost::asio::io_service& io_service,Setting &setting1)
            : socket_(io_service),setting(setting1)
    {
    }
private:
    boost::asio::streambuf data_;
    Setting &setting;


    void handle_write(const boost::system::error_code& /*error*/,
                      size_t /*bytes_transferred*/)
    {

    }
    void handle_read(const boost::system::error_code& ec){

        if (!ec){
            std::string word;
            char sp1, sp2, cr, lf;
            std::istream is(&data_);
            is.unsetf(std::ios_base::skipws);
            is >> word;
            std::cout << word << std::endl;
        }else{

        }

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
        tcp_connection::pointer newConnection = tcp_connection::create(acceptor_.get_io_service(),setting);
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

class session
        : public std::enable_shared_from_this<session>
{
public:
    session(tcp::socket socket)
            : socket_(std::move(socket))
    {
    }

    void start(Setting *setting1)
    {
        setting = setting1;
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
    void read_line_handler(const boost::system::error_code& ec,std::size_t size){

        if (ec)
            return;
        std::istream is(&buf_);
        std::string line;
        std::getline(is, line);
        std::cout <<"Get:" << line << std::endl;
        //-thread=[number],-read_percent=[number]
        std::string message = setting->baseSetting.setBaseSetting(line);
        do_write(message);
    }
    void do_write(std::string &message)
    {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(message,message.size()),
                                 [this, self](boost::system::error_code ec, std::size_t)
                                 {
                                 });
        std::cout << "Send:" << message << std::endl;
    }

    tcp::socket socket_;
    boost::asio::streambuf buf_;
    Setting *setting;
    std::unordered_map<std::string,int > map;
};
class Server
{
    Setting &setting;
public:
    Server(boost::asio::io_service& io_service, short port, Setting &setting1)
            : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
              socket_(io_service),
              setting(setting1)
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
                                       std::make_shared<session>(std::move(socket_))->start(&setting);
                                   }

                                   do_accept();
                               });
    }

    tcp::acceptor acceptor_;
    tcp::socket socket_;
};

#endif //TERARKDB_TEST_FRAMEWORK_TCPSERVER_H
