#include "client/FastRemote.h"
#include <stdexcept>  // std::runtime_error
#include <boost/asio/write.hpp>
#include "client/IOManager.h"


extern cardinality::IOManager *g_io_mgr;  // client.cpp

namespace cardinality {

FastRemote::FastRemote(const NodeID n, Operator::Ptr c,
                       const boost::asio::ip::address_v4 &i)
    : Remote(n, c, i)
{
}

FastRemote::~FastRemote()
{
}

Operator::Ptr FastRemote::clone() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

void FastRemote::Open(const char *, const uint32_t)
{
    using google::protobuf::io::CodedOutputStream;

    socket_ = g_io_mgr->connectSocket(child_->node_id(), ip_address_);
    buffer_.reset(new boost::asio::streambuf());

    uint32_t plan_size = child_->ByteSize();
    int total_size = 5 + plan_size;

    uint8_t *target = boost::asio::buffer_cast<uint8_t *>(
                          buffer_->prepare(total_size));

    *target++ = 'F';
    target = CodedOutputStream::WriteLittleEndian32ToArray(plan_size, target);
    target = child_->SerializeToArray(target);

    buffer_->commit(total_size);

    boost::asio::write(*socket_, *buffer_);
    buffer_->consume(total_size);
}

uint8_t *FastRemote::SerializeToArray(uint8_t *target) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

int FastRemote::ByteSize() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

}  // namespace cardinality
