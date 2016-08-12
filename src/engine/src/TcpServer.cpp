//
// Created by terark on 16-8-2.
//

#include "TcpServer.h"
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
    if (line == "query ops"){
        message = benchmark->GatherTimeData();
    }else {
        message = setting->baseSetting.setBaseSetting(line);
    }
    message += "\nEND\r\n";
    do_write(message);
}
void Session::do_write(std::string &message)
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
