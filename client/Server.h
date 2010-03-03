#ifndef CLIENT_SERVER_H_
#define CLIENT_SERVER_H_

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/shared_ptr.hpp>


namespace ca {

typedef boost::shared_ptr<boost::asio::ip::tcp::iostream> tcpstream_ptr;

class Server {
public:
    Server(const int port);
    ~Server();

    void run();
    void stop();

protected:
    void handle_accept(const boost::system::error_code &e);

    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    tcpstream_ptr new_tcpstream_;

private:
    Server(const Server &);
    Server& operator=(const Server &);
};

}  // namespace ca

#endif  // CLIENT_SERVER_H_
