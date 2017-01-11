//
// Created by terark on 16-8-2.
//

#include "TcpServer.h"

Session::Session(tcp::socket socket) : socket_(std::move(socket)) {
  setting = NULL;
  benchmark = NULL;
}

void Session::start(Setting *setting1, Benchmark *bm) {
    setting = setting1;
    benchmark = bm;
    do_read();
}

void Session::do_read(){
    auto self(shared_from_this());
    boost::asio::async_read_until(socket_, buf_, "\n",
		  [this,self](boost::system::error_code ec, std::size_t lenth)
		  {
			  read_line_handler(ec,lenth);
			  do_read();
		  });
}
void Session::read_line_handler(const boost::system::error_code& ec,std::size_t size) {
    if (ec)
        return;
    std::istream is(&buf_);
    std::string line;
    std::getline(is, line);
    std::cout << "Get:" << line << std::endl;
    std::string message;
    message = setting->setBaseSetting(line);
    message += "\nEND\r\n";
    do_write(message);
}

void Session::do_write(std::string &message) {
  auto self(shared_from_this());
  boost::asio::async_write(socket_, boost::asio::buffer(message,message.size()),
  [this, self,message](boost::system::error_code ec, std::size_t transferred_byte)
  {
    if (transferred_byte == message.size())
      std::cout << "Reply finish!" << std::endl;
    else {
      std::cout << "Send:" << transferred_byte << std::endl;
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
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  do_accept();
}

void Server::do_accept() {
  acceptor_.async_accept(socket_,
  [this](boost::system::error_code ec) {
    if (!ec){
      std::make_shared<Session>(std::move(socket_))->start(&setting,benchmark);
    }
    do_accept();
  });
}
