#include <boost/asio/io_service.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "Remote.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "Union.h"
#include "Server.h"


extern ca::Server *g_server; // client.cpp

namespace ca {

Remote::Remote(const NodeID n, Operator::Ptr c, const char *i)
    : Operator(n),
      child_(c), ip_address_(i),
      socket_(g_server->io_service()),
      line_buffer_(),
      response_()
{
    for (size_t i = 0; i < child_->numOutputCols(); ++i) {
        selected_input_col_ids_.push_back(i);
    }
}

Remote::Remote()
    : Operator(),
      child_(), ip_address_(),
      socket_(g_server->io_service()),
      line_buffer_(),
      response_()
{
}

Remote::Remote(const Remote &x)
    : Operator(x),
      child_(x.child_->clone()), ip_address_(x.ip_address_),
      socket_(g_server->io_service()),
      line_buffer_(),
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

RC Remote::Open(const char *left_ptr, const uint32_t left_len)
{
    line_buffer_.reset(new char[std::max(static_cast<size_t>(1),
                                       (MAX_VARCHAR_LEN + 1) * child_->numOutputCols())]);

    boost::asio::ip::tcp::endpoint endpoint
        = boost::asio::ip::tcp::endpoint(
              boost::asio::ip::address::from_string(ip_address_), 17000 + child_->getNodeID());

    boost::system::error_code error;
    socket_.connect(endpoint, error);
    if (error) {
        throw boost::system::system_error(error);
    }

    if (left_ptr) {
        boost::asio::streambuf request;
        std::ostream request_stream(&request);
        request_stream << "P";
        boost::archive::binary_oarchive oa(request_stream);
        oa.register_type(static_cast<IndexScan *>(NULL));

        oa << child_;
        request_stream << left_len << '\n';
        request_stream.write(left_ptr, left_len);
        boost::asio::write(socket_, request);

    } else {
        boost::asio::streambuf request;
        std::ostream request_stream(&request);
        request_stream << "Q";
        boost::archive::binary_oarchive oa(request_stream);
        oa.register_type(static_cast<NLJoin *>(NULL));
        oa.register_type(static_cast<NBJoin *>(NULL));
        oa.register_type(static_cast<SeqScan *>(NULL));
        oa.register_type(static_cast<IndexScan *>(NULL));
        oa.register_type(static_cast<Remote *>(NULL));
        oa.register_type(static_cast<Union *>(NULL));

        oa << child_;
        boost::asio::write(socket_, request);
    }

    return 0;
}

RC Remote::ReOpen(const char *left_ptr, const uint32_t left_len)
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

    return 0;
}

RC Remote::GetNext(Tuple &tuple)
{
    tuple.clear();
    try {
        boost::asio::read_until(socket_, response_, '\n');
    } catch (boost::system::system_error &e) {
        return -1;
    }
    std::istream is(&response_);
    is.getline(line_buffer_.get(),
               std::max(static_cast<size_t>(1),
                        (MAX_VARCHAR_LEN + 1) * child_->numOutputCols()));
    if (*line_buffer_.get() == '\0') {
        return 0;
    }
    if (*line_buffer_.get() == '|') {
        return -1;
    }

    const char *pos = line_buffer_.get();
#ifndef _GNU_SOURCE
    const char *eof = line_buffer_.get() + std::strlen(line_buffer_.get()) + 1;
#endif

    for (size_t i = 0; i < child_->numOutputCols(); ++i) {
#ifdef _GNU_SOURCE
        const char *delim = static_cast<const char *>(
                                rawmemchr(pos, (i == child_->numOutputCols() - 1) ? '\0' : '|'));
#else
        const char *delim = static_cast<const char *>(
                                std::memchr(pos, (i == child_->numOutputCols() - 1) ? '\0' : '|', eof - pos));
#endif
        tuple.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    return 0;
}

RC Remote::Close()
{
    socket_.close();
    line_buffer_.reset();

    return 0;
}

void Remote::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Remote@" << getNodeID();
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

}  // namespace ca
