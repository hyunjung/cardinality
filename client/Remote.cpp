#include "client/Remote.h"
#include <boost/asio/write.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/Union.h"
#include "client/Server.h"


extern cardinality::Server *g_server;  // client.cpp

namespace cardinality {

Remote::Remote(const NodeID n, Operator::Ptr c, const char *i)
    : Operator(n),
      child_(c),
      ip_address_(i),
      socket_(g_server->io_service()),
      response_()
{
    for (std::size_t i = 0; i < child_->numOutputCols(); ++i) {
        selected_input_col_ids_.push_back(i);
    }
}

Remote::Remote()
    : Operator(),
      child_(),
      ip_address_(),
      socket_(g_server->io_service()),
      response_()
{
}

Remote::Remote(const Remote &x)
    : Operator(x),
      child_(x.child_->clone()),
      ip_address_(x.ip_address_),
      socket_(g_server->io_service()),
      response_()
{
}

Remote::~Remote()
{
}

Operator::Ptr Remote::clone() const
{
    return Operator::Ptr(new Remote(*this));
}

void Remote::Open(const char *left_ptr, const uint32_t left_len)
{
    boost::asio::ip::tcp::endpoint endpoint
        = boost::asio::ip::tcp::endpoint(
              boost::asio::ip::address::from_string(ip_address_), 17000 + child_->node_id());

    boost::system::error_code error;
    socket_.connect(endpoint, error);
    if (error) {
        throw boost::system::system_error(error);
    }

    boost::asio::streambuf request;
    std::ostream request_stream(&request);

    if (left_ptr) {
        request_stream << "P";
        boost::archive::binary_oarchive oa(request_stream);
        oa.register_type(static_cast<IndexScan *>(NULL));

        oa << child_;
        request_stream << left_len << '\n';
        request_stream.write(left_ptr, left_len);

    } else {
        request_stream << "Q";
        boost::archive::binary_oarchive oa(request_stream);
        oa.register_type(static_cast<NLJoin *>(NULL));
        oa.register_type(static_cast<NBJoin *>(NULL));
        oa.register_type(static_cast<SeqScan *>(NULL));
        oa.register_type(static_cast<IndexScan *>(NULL));
        oa.register_type(static_cast<Remote *>(NULL));
        oa.register_type(static_cast<Union *>(NULL));

        oa << child_;
    }

    boost::asio::write(socket_, request);
}

void Remote::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    if (left_ptr) {
        boost::asio::streambuf request;
        std::ostream request_stream(&request);
        request_stream << left_len << '\n';
        request_stream.write(left_ptr, left_len);
        boost::asio::write(socket_, request);
    } else {
        socket_.close();
        Open();
    }
}

bool Remote::GetNext(Tuple &tuple)
{
    boost::system::error_code error;
    std::size_t len = boost::asio::read_until(socket_, response_, '\n', error);
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

#ifndef _GNU_SOURCE
    const char *eof = pos + len;
#endif

    for (std::size_t i = 0; i < child_->numOutputCols(); ++i) {
#ifdef _GNU_SOURCE
        const char *delim
            = static_cast<const char *>(
                  rawmemchr(pos, (i == child_->numOutputCols() - 1) ? '\n' : '|'));
#else
        const char *delim
            = static_cast<const char *>(
                  std::memchr(pos, (i == child_->numOutputCols() - 1) ? '\n' : '|', eof - pos));
#endif
        tuple.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    return false;
}

void Remote::Close()
{
    socket_.close();
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

ValueType Remote::getColType(const char *col) const
{
    return child_->getColType(col);
}

double Remote::estCost() const
{
    return child_->estCost() + COST_NET_XFER_BYTE * estTupleLength() * estCardinality();
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
