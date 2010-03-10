#ifndef CLIENT_SERVER_H_
#define CLIENT_SERVER_H_

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/shared_ptr.hpp>


namespace cardinality {

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

class Server {
public:
    explicit Server(const int port);
    ~Server();

    void run();
    void stop();
    boost::asio::io_service & io_service();

protected:
    void handle_accept(const boost::system::error_code &e);

    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    socket_ptr new_socket_;

private:
    Server(const Server &);
    Server& operator=(const Server &);
};

}  // namespace cardinality

#endif  // CLIENT_SERVER_H_
