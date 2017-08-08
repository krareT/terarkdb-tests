//
// Created by terark on 16-8-2.
//

#include "TcpServer.h"
#include <boost/algorithm/string.hpp>

Session::Session(tcp::socket socket) : socket_(std::move(socket)) {
  setting = NULL;
  benchmark = NULL;
}

/**
 * 开始Session事务
 * 通过socket异步的读取数据
 * 再通过socket异步的写数据
 * @param setting1
 * @param bm
 */
void Session::start(Setting *setting1, Benchmark *bm) {
    setting = setting1;
    benchmark = bm;
    do_read();
}

/**
 * 通过socket异步的读取数据直到遇到换行符为止
 * 然后重复do_read
 */
void Session::do_read(){
    auto self(shared_from_this());
    boost::asio::async_read_until(socket_, buf_, "\n",
		  [this,self](boost::system::error_code ec, std::size_t lenth)
		  {
			  read_line_handler(ec,lenth);
			  do_read();
		  });
}

/**
 * 从buf_中读取一行
 * 然后根据这一行设置Setting，并获取配置的message
 * 最后将这个message通过socket异步传输
 * @param ec
 * @param size
 */
void Session::read_line_handler(const boost::system::error_code& ec,std::size_t size) {
    if (ec)
        return;
    std::istream is(&buf_);
    std::string line;
    std::getline(is, line);
    fprintf(stderr, "Get: %s\n", line.c_str());
    std::string message;
    message = setting->setBaseSetting(line);
    message += "\nEND\r\n";
    do_write(message);
}

/**
 * 通过socket异步的写数据
 * 如果传输的字节数与message大小不一致，则继续执行do_write()操作
 * 否则结束
 * @param message
 */
void Session::do_write(std::string &message) {
  auto self(shared_from_this());
  boost::asio::async_write(socket_, boost::asio::buffer(message,message.size()),
  [this, self,message](boost::system::error_code ec, std::size_t transferred_byte)
  {
    if (transferred_byte == message.size())
      fprintf(stderr, "Reply finish!\n");
    else {
      fprintf(stderr, "Send: %zd\n",  transferred_byte);
      std::string msg = message.substr(transferred_byte);
      do_write(msg);
    }
  });
}

Server::Server(boost::asio::io_service& io_service, short port,
               Setting &setting1, Benchmark *bm)
  : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
    socket_(io_service),
    setting(setting1),
    benchmark(bm)
{
  acceptor_.set_option(tcp::acceptor::reuse_address(true)); // 设置socket允许被绑定到一个正在使用的address中
  do_accept();
}

/**
 * 开始异步的接收socket连接
 * 并用Session.start()开始事务
 * 重复执行do_accept()
 */
void Server::do_accept() {
  acceptor_.async_accept(socket_,
  [this](boost::system::error_code ec) {
    if (!ec){
      /**
       * 等价于：
       * Session* pointer = std::make_shared<Session>(std::move(socket_));
       * pointer->start(&setting, benchmark);
       * 等价于
       * Session* pointer = new Session(socket_); (大致意思等价，细节差距很大)
       */
      std::make_shared<Session>(std::move(socket_))->start(&setting,benchmark);
    }
    do_accept();
  });
}
