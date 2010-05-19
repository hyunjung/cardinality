#ifndef CLIENT_IOMANAGER_H_
#define CLIENT_IOMANAGER_H_

#include <string>
#include <utility>  // std::pair
#include <tr1/unordered_map>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/mutex.hpp>
#include "client/Connection.h"


namespace cardinality {

typedef uint32_t NodeID;  // Operator.h
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;
typedef boost::shared_ptr<boost::iostreams::mapped_file_source> mapped_file_ptr;

class IOManager {
public:
    explicit IOManager(const NodeID);
    ~IOManager();

    void run();

    tcpsocket_ptr connectSocket(const NodeID,
                                const boost::asio::ip::address_v4 &);
    void closeSocket(const NodeID, tcpsocket_ptr);

    std::pair<const char *, const char *> openFile(const std::string &);

private:
    IOManager(const IOManager &);
    IOManager& operator=(const IOManager &);

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

#endif  // CLIENT_IOMANAGER_H_
