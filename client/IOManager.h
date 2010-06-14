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

#endif  // CARDINALITY_IOMANAGER_H_
