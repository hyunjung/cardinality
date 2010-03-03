#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "Remote.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "Union.h"


namespace ca {

Remote::Remote(const NodeID n, Operator::Ptr c, const char *i)
    : Operator(n),
      child_(c), hostname_(i),
      tcpstream_(), line_buffer_()
{
    for (size_t i = 0; i < child_->numOutputCols(); ++i) {
        selected_input_col_ids_.push_back(i);
    }
}

Remote::Remote()
    : Operator(),
      child_(), hostname_(),
      tcpstream_(), line_buffer_()
{
}

Remote::Remote(const Remote &x)
    : Operator(x),
      child_(x.child_->clone()), hostname_(x.hostname_),
      tcpstream_(), line_buffer_()
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

    tcpstream_.connect(hostname_, boost::lexical_cast<std::string>(17000 + child_->getNodeID()));
    if (tcpstream_.fail()) {
        throw std::runtime_error("tcp::iostream.connect() failed");
    }

    if (left_ptr) {
        tcpstream_.put('P');

        boost::archive::binary_oarchive oa(tcpstream_);
        oa.register_type(static_cast<IndexScan *>(NULL));

        oa << child_;

        tcpstream_ << left_len << '\n';
        tcpstream_.write(left_ptr, left_len);
        tcpstream_ << std::flush;

    } else {
        tcpstream_.put('Q');

        boost::archive::binary_oarchive oa(tcpstream_);
        oa.register_type(static_cast<NLJoin *>(NULL));
        oa.register_type(static_cast<NBJoin *>(NULL));
        oa.register_type(static_cast<SeqScan *>(NULL));
        oa.register_type(static_cast<IndexScan *>(NULL));
        oa.register_type(static_cast<Remote *>(NULL));
        oa.register_type(static_cast<Union *>(NULL));

        oa << child_;
    }

    return 0;
}

RC Remote::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    if (left_ptr) {
        tcpstream_ << left_len << '\n';
        tcpstream_.write(left_ptr, left_len);
        tcpstream_ << std::flush;
    } else {
        tcpstream_.close();
        Open();
    }

    return 0;
}

RC Remote::GetNext(Tuple &tuple)
{
    tuple.clear();
    tcpstream_.getline(line_buffer_.get(),
                      std::max(static_cast<size_t>(1),
                               (MAX_VARCHAR_LEN + 1) * child_->numOutputCols()));
    if (*line_buffer_.get() == '\0') {
        return (tcpstream_.eof()) ? -1 : 0;
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
    tcpstream_.close();
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
