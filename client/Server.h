#ifndef CLIENT_SERVER_H_
#define CLIENT_SERVER_H_

#include <tr1/unordered_map>
#include <boost/thread/mutex.hpp>
#include "client/Connection.h"


namespace cardinality {

typedef int NodeID;
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;

class Server {
public:
    explicit Server(const int);
    ~Server();

    void run();

    tcpsocket_ptr connectSocket(const NodeID, const std::string &);
    void closeSocket(const NodeID, tcpsocket_ptr);

private:
    Server(const Server &);
    Server& operator=(const Server &);

    void handle_accept(const boost::system::error_code &);

    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    Connection::Ptr new_connection_;
    std::tr1::unordered_multimap<NodeID, tcpsocket_ptr> connection_pool_;
    boost::mutex mutex_;
};

}  // namespace cardinality

#endif  // CLIENT_SERVER_H_
