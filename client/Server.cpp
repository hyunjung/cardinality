#include "client/Server.h"
#include <boost/bind/bind.hpp>
#include <boost/asio/placeholders.hpp>


namespace cardinality {

Server::Server(const int port)
    : io_service_(),
      acceptor_(io_service_,
                boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(),
                                               port)),
      new_connection_(new Connection(io_service_)),
      connection_pool_(),
      mutex_()
{
    acceptor_.async_accept(new_connection_->socket(),
                           boost::bind(&Server::handle_accept, this,
                                       boost::asio::placeholders::error));
}

Server::~Server()
{
}

void Server::run()
{
    io_service_.run();
}

void Server::handle_accept(const boost::system::error_code &e)
{
    if (e) {
        throw boost::system::system_error(e);
    }

    new_connection_->start();
    new_connection_.reset(new Connection(io_service_));
    acceptor_.async_accept(new_connection_->socket(),
                           boost::bind(&Server::handle_accept, this,
                                       boost::asio::placeholders::error));
}

tcpsocket_ptr Server::connectSocket(const NodeID node_id,
                                    const std::string &ip_address)
{
    tcpsocket_ptr socket;

    mutex_.lock();
    std::tr1::unordered_multimap<NodeID, tcpsocket_ptr>::iterator it
        = connection_pool_.find(node_id);

    if (it == connection_pool_.end()) {
        mutex_.unlock();
        boost::asio::ip::tcp::endpoint endpoint
            = boost::asio::ip::tcp::endpoint(
                  boost::asio::ip::address::from_string(ip_address),
                  17000 + node_id);

        socket.reset(new boost::asio::ip::tcp::socket(io_service_));

        boost::system::error_code error;
        socket->connect(endpoint, error);
        if (error) {
            throw boost::system::system_error(error);
        }

    } else {  // reuse a pooled connection
        socket = it->second;
        connection_pool_.erase(it);
        mutex_.unlock();
    }

    return socket;
}

void Server::closeSocket(const NodeID node_id, tcpsocket_ptr sock)
{
    mutex_.lock();
    connection_pool_.insert(std::pair<NodeID, tcpsocket_ptr>(node_id, sock));
    mutex_.unlock();
}

}  // namespace cardinality
