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

#ifndef CARDINALITY_REMOTE_H_
#define CARDINALITY_REMOTE_H_

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include "client/Operator.h"


namespace cardinality {

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;

class Remote: public Operator {
public:
    // constructor, destructor
    Remote(const NodeID, Operator::Ptr,
           const boost::asio::ip::address_v4 &);
    explicit Remote(google::protobuf::io::CodedInputStream *);
    Remote(const Remote &);
    ~Remote();
    Operator::Ptr clone() const;

    // query execution
    void Open(const Chunk * = NULL);
    void ReOpen(const Chunk * = NULL);
    bool GetNext(Tuple &);
    void Close();

    // serialization
    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    // plan exploration
    void print(std::ostream &, const int, const double) const;
    bool hasCol(const ColName) const;
    ColID getInputColID(const ColName) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const ColName) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const ColName) const;

    // cost estimation
    double estCost(const double = 0.0) const;
    double estCardinality(const bool = false) const;
    double estTupleSize() const;
    double estColSize(const ColID) const;

protected:
    // operator description
    Operator::Ptr child_;
    boost::asio::ip::address_v4 ip_address_;

    // execution states
    bool socket_reuse_;
    tcpsocket_ptr socket_;
    boost::scoped_ptr<boost::asio::streambuf> buffer_;

    // constants
    static const double COST_NET_XFER_BYTE = 0.0025;

private:
    Remote& operator=(const Remote &);
};

}  // namespace cardinality

#endif  // CARDINALITY_REMOTE_H_
