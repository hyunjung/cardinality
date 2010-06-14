// Copyright (c) 2010, Hyunjung Park
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Stanford University nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "client/IOManager.h"
#include <boost/bind/bind.hpp>
#include <boost/asio/placeholders.hpp>


namespace cardinality {

IOManager::IOManager(const NodeID n)
    : io_service_(),
      acceptor_(io_service_),
      new_connection_(new Connection(io_service_)),
      connection_pool_(), connpool_mutex_(),
      files_(), files_mutex_()
{
    boost::asio::ip::tcp::endpoint port(boost::asio::ip::tcp::v4(), 17000 + n);
    acceptor_.open(port.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(port);
    acceptor_.listen(1024);
    acceptor_.async_accept(new_connection_->socket(),
                           boost::bind(&IOManager::handle_accept, this,
                                       boost::asio::placeholders::error));
}

IOManager::~IOManager()
{
}

void IOManager::run()
{
    io_service_.run();
}

void IOManager::handle_accept(const boost::system::error_code &e)
{
    if (e) {
        throw boost::system::system_error(e);
    }

    new_connection_->start();
    new_connection_.reset(new Connection(io_service_));
    acceptor_.async_accept(new_connection_->socket(),
                           boost::bind(&IOManager::handle_accept, this,
                                       boost::asio::placeholders::error));
}

tcpsocket_ptr IOManager::connectSocket(const NodeID node_id,
                                       const boost::asio::ip::address_v4 &addr)
{
    tcpsocket_ptr socket;

    connpool_mutex_.lock();
    std::tr1::unordered_multimap<NodeID, tcpsocket_ptr>::iterator it
        = connection_pool_.find(node_id);

    if (it == connection_pool_.end()) {
        connpool_mutex_.unlock();

        socket.reset(new boost::asio::ip::tcp::socket(io_service_));

        boost::system::error_code error;
        socket->connect(boost::asio::ip::tcp::endpoint(addr, 17000 + node_id),
                        error);
        if (error) {
            throw boost::system::system_error(error);
        }

    } else {  // reuse a pooled connection
        socket = it->second;
        connection_pool_.erase(it);
        connpool_mutex_.unlock();
    }

    return socket;
}

void IOManager::closeSocket(const NodeID node_id, tcpsocket_ptr sock)
{
    connpool_mutex_.lock();
    connection_pool_.insert(std::pair<NodeID, tcpsocket_ptr>(node_id, sock));
    connpool_mutex_.unlock();
}

std::pair<const char *, const char *>
IOManager::openFile(const std::string &filename)
{
    mapped_file_ptr file;

    files_mutex_.lock();
    std::tr1::unordered_map<std::string, mapped_file_ptr>::iterator it
        = files_.find(filename);
    if (it == files_.end()) {
        file.reset(new boost::iostreams::mapped_file_source(filename));
        files_[filename] = file;
    } else {
        file = it->second;
    }
    files_mutex_.unlock();

    return std::make_pair(file->begin(), file->end());
}

}  // namespace cardinality
