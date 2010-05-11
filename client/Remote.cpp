#include "client/Remote.h"
#include <boost/asio/write.hpp>
#include <boost/asio/read_until.hpp>
#include "client/IOManager.h"


extern cardinality::IOManager *g_io_mgr;  // client.cpp

namespace cardinality {

Remote::Remote(const NodeID n, Operator::Ptr c,
               const boost::asio::ip::address_v4 &i)
    : Operator(n),
      ip_address_(i),
      child_(c),
      socket_(),
      buffer_()
{
}

Remote::Remote()
    : Operator(),
      ip_address_(),
      child_(),
      socket_(),
      buffer_()
{
}

Remote::Remote(const Remote &x)
    : Operator(x),
      ip_address_(x.ip_address_),
      child_(x.child_->clone()),
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

void Remote::Open(const char *, const uint32_t)
{
    socket_ = g_io_mgr->connectSocket(child_->node_id(), ip_address_);
    buffer_.reset(new boost::asio::streambuf());

    ReOpen();
}

void Remote::ReOpen(const char *, const uint32_t)
{
    using google::protobuf::io::CodedOutputStream;

    uint32_t plan_size = child_->ByteSize();
    int total_size = 5 + plan_size;

    uint8_t *target = boost::asio::buffer_cast<uint8_t *>(
                          buffer_->prepare(total_size));

    *target++ = 'Q';
    target = CodedOutputStream::WriteLittleEndian32ToArray(plan_size, target);
    target = child_->SerializeToArray(target);

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
    g_io_mgr->closeSocket(child_->node_id(), socket_);
    socket_.reset();
}

uint8_t *Remote::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(5, target);

    target = CodedOutputStream::WriteVarint32ToArray(node_id_, target);

    target = CodedOutputStream::WriteLittleEndian64ToArray(
                 ip_address_.to_ulong(), target);
    target = child_->SerializeToArray(target);

    return target;
}

int Remote::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = 1;

    total_size += CodedOutputStream::VarintSize32(node_id_);

    total_size += 8;
    total_size += child_->ByteSize();

    return total_size;
}

void Remote::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    input->ReadVarint32(&node_id_);

    unsigned long addr;
    input->ReadLittleEndian64(&addr);
    ip_address_ = boost::asio::ip::address_v4(addr);
    child_ = parsePlan(input);
}

void Remote::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Remote@" << node_id();
    os << " cost=" << estCost();
    os << std::endl;

    child_->print(os, tab + 1);
}

bool Remote::hasCol(const char *col) const
{
    return child_->hasCol(col);
}

ColID Remote::getInputColID(const char *col) const
{
    return child_->getOutputColID(col);
}

ColID Remote::getBaseColID(const ColID cid) const
{
    return child_->getBaseColID(cid);
}

const PartStats *Remote::getPartStats(const char *col) const
{
    return child_->getPartStats(col);
}

ValueType Remote::getColType(const char *col) const
{
    return child_->getColType(col);
}

ColID Remote::numOutputCols() const
{
    return child_->numOutputCols();
}

ColID Remote::getOutputColID(const char *col) const
{
    return child_->getOutputColID(col);
}

double Remote::estCost(const double) const
{
    return child_->estCost()
           + COST_NET_XFER_BYTE * estTupleLength() * estCardinality();
}

double Remote::estCardinality() const
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
