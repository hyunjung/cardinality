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

#include "client/Remote.h"
#include <cstring>
#include <boost/asio/write.hpp>
#include <boost/asio/read_until.hpp>
#include "client/IOManager.h"


extern cardinality::IOManager *g_io_mgr;  // client.cpp

namespace cardinality {

Remote::Remote(const NodeID n, Operator::Ptr c,
               const boost::asio::ip::address_v4 &i)
    : Operator(n),
      child_(c),
      ip_address_(i),
      socket_reuse_(),
      socket_(),
      buffer_()
{
}

Remote::Remote(google::protobuf::io::CodedInputStream *input)
    : Operator(input),
      child_(),
      ip_address_(),
      socket_reuse_(),
      socket_(),
      buffer_()
{
    Deserialize(input);
}

Remote::Remote(const Remote &x)
    : Operator(x),
      child_(x.child_->clone()),
      ip_address_(x.ip_address_),
      socket_reuse_(),
      socket_(),
      buffer_()
{
}

Remote::~Remote()
{
}

Operator::Ptr Remote::clone() const
{
    return boost::make_shared<Remote>(*this);
}

void Remote::Open(const char *left_ptr, const uint32_t left_len)
{
    socket_ = g_io_mgr->connectSocket(child_->node_id(), ip_address_);
    buffer_.reset(new boost::asio::streambuf());

    ReOpen(left_ptr, left_len);
}

void Remote::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    using google::protobuf::io::CodedOutputStream;

    socket_reuse_ = false;

    uint32_t plan_size = child_->ByteSize();
    int total_size = 5 + plan_size;
    if (left_ptr) {
        total_size += 4 + left_len;
    }

    uint8_t *target = boost::asio::buffer_cast<uint8_t *>(
                          buffer_->prepare(total_size));

    *target++ = (!left_ptr) ? 'Q' : 'P';

    target = CodedOutputStream::WriteLittleEndian32ToArray(plan_size, target);
    target = child_->SerializeToArray(target);

    if (left_ptr) {
        target = CodedOutputStream::WriteLittleEndian32ToArray(
                     left_len, target);
        target = CodedOutputStream::WriteRawToArray(
                     left_ptr, left_len, target);
    }

    buffer_->commit(total_size);

    boost::asio::write(*socket_, *buffer_);
    buffer_->consume(total_size);
}

bool Remote::GetNext(Tuple &tuple)
{
    boost::system::error_code error;
    std::size_t size = boost::asio::read_until(*socket_, *buffer_, '\n', error);
    if (error) {
        if (error == boost::asio::error::eof) {
            return true;
        }
        throw boost::system::system_error(error);
    }

    tuple.clear();

    if (size == 0) {
        return false;
    }

    boost::asio::streambuf::const_buffers_type line_buffer = buffer_->data();
    buffer_->consume(size);
    const char *pos = boost::asio::buffer_cast<const char *>(line_buffer);

    if (*pos == '|') {
        socket_reuse_ = true;
        return true;
    }

    for (ColID i = 0; i < child_->numOutputCols(); ++i) {
        const char *delim
            = static_cast<const char *>(
                  rawmemchr(pos,
                            (i == child_->numOutputCols() - 1) ? '\n' : '|'));
        tuple.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    return false;
}

void Remote::Close()
{
    buffer_.reset();

    if (socket_reuse_) {
        g_io_mgr->closeSocket(child_->node_id(), socket_);
    } else {
        socket_->close();
    }
    socket_.reset();
}

uint8_t *Remote::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(TAG_REMOTE, target);

    target = Operator::SerializeToArray(target);

    target = CodedOutputStream::WriteLittleEndian64ToArray(
                 ip_address_.to_ulong(), target);
    target = child_->SerializeToArray(target);

    return target;
}

int Remote::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = 1 + Operator::ByteSize();

    total_size += 8;
    total_size += child_->ByteSize();

    return total_size;
}

void Remote::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    unsigned long addr;
    input->ReadLittleEndian64(&addr);
    ip_address_ = boost::asio::ip::address_v4(addr);
    child_ = parsePlan(input);
}

#ifdef PRINT_PLAN
void Remote::print(std::ostream &os, const int tab, const double lcard) const
{
    os << std::string(4 * tab, ' ');
    os << "Remote@" << node_id();
    os << " cost=" << estCost(lcard);
    os << std::endl;

    child_->print(os, tab + 1, lcard);
}
#endif

bool Remote::hasCol(const ColName col) const
{
    return child_->hasCol(col);
}

ColID Remote::getInputColID(const ColName col) const
{
    return child_->getOutputColID(col);
}

std::pair<const PartStats *, ColID> Remote::getPartStats(const ColID cid) const
{
    return child_->getPartStats(cid);
}

ValueType Remote::getColType(const ColName col) const
{
    return child_->getColType(col);
}

ColID Remote::numOutputCols() const
{
    return child_->numOutputCols();
}

ColID Remote::getOutputColID(const ColName col) const
{
    return child_->getOutputColID(col);
}

double Remote::estCost(const double lcard) const
{
    return child_->estCost(lcard)
           + COST_NET_XFER_BYTE
             * child_->estTupleLength()
             * child_->estCardinality(lcard > 0.0);
}

double Remote::estCardinality(const bool) const
{
    return child_->estCardinality();
}

double Remote::estTupleLength() const
{
    return child_->estTupleLength();
}

double Remote::estColLength(const ColID cid) const
{
    return child_->estColLength(cid);
}

}  // namespace cardinality
