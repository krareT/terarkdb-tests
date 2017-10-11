//
// Created by terark on 17-8-10.
//

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <boost/asio.hpp>
#include <string>

using boost::asio::ip::tcp;

enum { max_length = 1024 };

int main(int argc, char *argv[]) {
    try {
      if (argc != 3) {
          std::cerr << "Usage: blocking_tcp_echo_client <host> <port>\n";
          return 1;
      }
      boost::asio::io_service io_service;
      tcp::socket s(io_service);
      tcp::resolver resolver(io_service);
      boost::asio::connect(s, resolver.resolve({argv[1], argv[2]}));
      std::string line;
      std::cout << "Enter command:";
      while (getline(std::cin, line)) {
          line += '\n';
          boost::asio::write(s, boost::asio::buffer(line, line.size()));
          boost::asio::streambuf buf;
          boost::asio::read_until(s, buf, "END\r\n");
          std::istream is(&buf);
          std::cout << "-------------------------\n";
          while (getline(is, line)) {
              std::cout << line << std::endl;
          }
          std::cout << "Enter command:";
      }
    }
    catch (std::exception &e) {
          std::cerr << "Exception: " << e.what() << "\n";
    }
    return 0;
}