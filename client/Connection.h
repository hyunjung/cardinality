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

#ifndef CARDINALITY_CONNECTION_H_
#define CARDINALITY_CONNECTION_H_

#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>


namespace cardinality {

// Represent a passive TCP connection handled by IOManager
class Connection: public boost::enable_shared_from_this<Connection> {
public:
    typedef boost::shared_ptr<Connection> Ptr;

    // constructor, destructor
    explicit Connection(boost::asio::io_service &);
    ~Connection();

    // Start waiting for a request.
    void start();

    // accessor
    boost::asio::ip::tcp::socket & socket();

private:
    // non-copyable
    Connection(const Connection &);
    Connection& operator=(const Connection &);

    // Asynchronous callback for a new request.
    // Calls one of the next three methods to handle the request.
    void handle_read(const boost::system::error_code &, std::size_t);

    // Receive a plan, execute it, and send the results back.
    void handle_query();

    // handle_query() with a join value for nested-loop index join.
    void handle_param_query();

    // Process a statistics gathering request.
    void handle_stats();

    // boost::asio
    boost::asio::ip::tcp::socket socket_;

    // buffer for receiving a request type
    char buffer_[1];
};

}  // namespace cardinality

#endif  // CARDINALITY_CONNECTION_H_
