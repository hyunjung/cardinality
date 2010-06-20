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

#ifndef CARDINALITY_IOMANAGER_H_
#define CARDINALITY_IOMANAGER_H_

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

// Singleton class that manages file and network IO.
class IOManager {
public:
    // service management
    static void start(const NodeID);
    static void stop();
    static IOManager *instance();

    // Get a TCP connection to another node.
    // Reuse a pooled connection if possible.
    // Otherwise, establish a new TCP connection.
    tcpsocket_ptr connectSocket(const NodeID,
                                const boost::asio::ip::address_v4 &);

    // Keep a TCP connection for reuse.
    void closeSocket(const NodeID, tcpsocket_ptr);

    // Open a file using memory-mapped IO.
    // Return start and end addresses.
    // Multiple calls to openFile return the same addresses.
    std::pair<const char *, const char *> openFile(const std::string &);

private:
    // constructor, destructor
    explicit IOManager(const NodeID);
    ~IOManager();

    // non-copyable
    IOManager(const IOManager &);
    IOManager& operator=(const IOManager &);

    // wrapper for io_service.run()
    void run();

    // Asynchronous callback for establishing an incoming TCP connection.
    // Call Connection::start() to handle the connection in a new thread.
    void handle_accept(const boost::system::error_code &);

    // boost::asio
    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    Connection::Ptr new_connection_;

    // connection pool
    std::tr1::unordered_multimap<NodeID, tcpsocket_ptr> connection_pool_;
    boost::mutex connpool_mutex_;

    // open files
    std::tr1::unordered_map<std::string, mapped_file_ptr> files_;
    boost::mutex files_mutex_;

    // singletone instance
    static IOManager *instance_;
};

}  // namespace cardinality

#endif  // CARDINALITY_IOMANAGER_H_
