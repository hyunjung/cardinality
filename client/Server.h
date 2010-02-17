#ifndef SERVER_H
#define SERVER_H

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>


typedef boost::shared_ptr<boost::asio::ip::tcp::iostream> tcpstream_ptr;

class Server {
public:
    Server(const int port);
    ~Server();

    void run();
    void stop();

protected:
    void handle_accept(const boost::system::error_code &e);

    boost::asio::io_service io_service;
    boost::asio::ip::tcp::acceptor acceptor;
    tcpstream_ptr new_tcpstream;

private:
    Server(const Server &);
    Server& operator=(const Server &);
};

#endif
