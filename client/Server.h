#ifndef CLIENT_SERVER_H_
#define CLIENT_SERVER_H_

#include <tr1/unordered_map>
#include <boost/thread/mutex.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include "client/Connection.h"


namespace cardinality {

typedef uint16_t NodeID;  // Operator.h
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;
typedef boost::shared_ptr<boost::iostreams::mapped_file_source> mapped_file_ptr;

class Server {
public:
    explicit Server(const int);
    ~Server();

    void run();

    tcpsocket_ptr connectSocket(const NodeID,
                                const boost::asio::ip::address_v4 &);
    void closeSocket(const NodeID, tcpsocket_ptr);

    std::pair<const char *, const char *> openFile(const std::string &);

private:
    Server(const Server &);
    Server& operator=(const Server &);

    void handle_accept(const boost::system::error_code &);

    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    Connection::Ptr new_connection_;

    std::tr1::unordered_multimap<NodeID, tcpsocket_ptr> connection_pool_;
    boost::mutex connpool_mutex_;
    std::tr1::unordered_map<std::string, mapped_file_ptr> files_;
    boost::mutex files_mutex_;
};

}  // namespace cardinality

#endif  // CLIENT_SERVER_H_
