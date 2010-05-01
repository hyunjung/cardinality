#ifndef CLIENT_CONNECTION_H_
#define CLIENT_CONNECTION_H_

#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>


namespace cardinality {

class Connection: public boost::enable_shared_from_this<Connection> {
public:
    typedef boost::shared_ptr<Connection> Ptr;

    explicit Connection(boost::asio::io_service &);
    ~Connection();

    void start();
    boost::asio::ip::tcp::socket & socket();

private:
    Connection(const Connection &);
    Connection& operator=(const Connection &);

    void handle_read(const boost::system::error_code &, std::size_t);
    void handle_query();
    void handle_stats();

    boost::asio::ip::tcp::socket socket_;
    char buffer_[1];
};

}  // namespace cardinality

#endif  // CLIENT_CONNECTION_H_
