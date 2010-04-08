#include "client/Remote.h"
#include <iomanip>
#include <boost/asio/write.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "client/Server.h"


extern cardinality::Server *g_server;  // client.cpp

namespace cardinality {

Remote::Remote(const NodeID n, Operator::Ptr c, const char *i)
    : Operator(n),
      child_(c),
      ip_address_(i),
      socket_(),
      socket_reusable_(),
      response_()
{
}

Remote::Remote()
    : Operator(),
      child_(),
      ip_address_(),
      socket_(),
      socket_reusable_(),
      response_()
{
}

Remote::Remote(const Remote &x)
    : Operator(x),
      child_(x.child_->clone()),
      ip_address_(x.ip_address_),
      socket_(),
      socket_reusable_(),
      response_()
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
    socket_ = g_server->connectSocket(child_->node_id(), ip_address_);

    std::ostringstream header_stream;
    boost::asio::streambuf body;
    std::ostream body_stream(&body);

    if (left_ptr) {
        boost::archive::binary_oarchive oa(body_stream,
                                           boost::archive::no_header
                                           | boost::archive::no_codecvt);
        oa << child_;

        header_stream << 'P';
        header_stream << std::setw(4) << std::hex << body.size();

        body_stream << std::setw(4) << std::hex << left_len;
        body_stream.write(left_ptr, left_len);

        socket_reusable_ = false;

    } else {
        boost::archive::binary_oarchive oa(body_stream,
                                           boost::archive::no_header
                                           | boost::archive::no_codecvt);
        oa << child_;

        header_stream << 'Q';
        header_stream << std::setw(4) << std::hex << body.size();

        socket_reusable_ = true;
    }

    std::vector<boost::asio::const_buffer> buffers;
    std::string header = header_stream.str();
    buffers.push_back(boost::asio::buffer(header));
    buffers.push_back(body.data());
    boost::asio::write(*socket_, buffers);
}

void Remote::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    if (left_ptr) {
        boost::asio::streambuf body;
        std::ostream body_stream(&body);
        body_stream << std::setw(4) << std::hex << left_len;
        body_stream.write(left_ptr, left_len);
        boost::asio::write(*socket_, body);
    } else {
        Close();
        Open();
    }
}

bool Remote::GetNext(Tuple &tuple)
{
    boost::system::error_code error;
    std::size_t len = boost::asio::read_until(*socket_, response_, '\n', error);
    if (error) {
        if (error == boost::asio::error::eof) {
            return true;
        }
        throw boost::system::system_error(error);
    }

    tuple.clear();

    if (len == 0) {
        return false;
    }

    boost::asio::streambuf::const_buffers_type line_buffer = response_.data();
    response_.consume(len);
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
    if (socket_reusable_) {
        g_server->closeSocket(child_->node_id(), socket_);
    } else {
        socket_->close();
    }
    socket_.reset();
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

const PartitionStats *Remote::getPartitionStats(const char *col) const
{
    return child_->getPartitionStats(col);
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

double Remote::estCost(const double left_cardinality) const
{
    return child_->estCost(left_cardinality)
           + 2 * COST_NET_XFER_PKT * left_cardinality
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
