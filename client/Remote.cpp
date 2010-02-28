#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "Remote.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "Union.h"

using namespace ca;


Remote::Remote(const NodeID n, Operator::Ptr c, const char *i)
    : Operator(n), child(c), ipAddress(i), tcpstream(), lineBuffer()
{
    for (size_t i = 0; i < child->numOutputCols(); ++i) {
        selectedInputColIDs.push_back(i);
    }
}

Remote::Remote()
    : child(), ipAddress(), tcpstream(), lineBuffer()
{
}

Remote::~Remote()
{
}

RC Remote::Open(const char *, const uint32_t)
{
    lineBuffer.reset(new char[std::max(static_cast<size_t>(1),
                                       (MAX_VARCHAR_LEN + 1) * child->numOutputCols())]);

    tcpstream.connect(ipAddress, boost::lexical_cast<std::string>(17000 + child->getNodeID()));
    if (tcpstream.fail()) {
        throw std::runtime_error("tcp::iostream.connect() failed");
    }

    boost::archive::binary_oarchive oa(tcpstream);
    oa.register_type(static_cast<NLJoin *>(NULL));
    oa.register_type(static_cast<NBJoin *>(NULL));
    oa.register_type(static_cast<SeqScan *>(NULL));
    oa.register_type(static_cast<IndexScan *>(NULL));
    oa.register_type(static_cast<Remote *>(NULL));
    oa.register_type(static_cast<Union *>(NULL));

    oa << child;
    return 0;
}

RC Remote::GetNext(Tuple &tuple)
{
    tuple.clear();
    tcpstream.getline(lineBuffer.get(),
                      std::max(static_cast<size_t>(1),
                               (MAX_VARCHAR_LEN + 1) * child->numOutputCols()));
    if (*lineBuffer.get() == '\0') {
        return (tcpstream.eof()) ? -1 : 0;
    }

    const char *pos = lineBuffer.get();
    const char *eof = lineBuffer.get() + std::strlen(lineBuffer.get()) + 1;

    for (size_t i = 0; i < child->numOutputCols(); ++i) {
        const char *delim = static_cast<const char *>(
                                std::memchr(pos, (i == child->numOutputCols() - 1) ? '\0' : '|', eof - pos));
        tuple.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    return 0;
}

RC Remote::Close()
{
    tcpstream.close();
    lineBuffer.reset();

    return 0;
}

void Remote::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Remote@" << getNodeID();
    os << " cost=" << estCost();
    os << std::endl;

    child->print(os, tab + 1);
}

bool Remote::hasCol(const char *col) const
{
    return child->hasCol(col);
}

ColID Remote::getInputColID(const char *col) const
{
    return child->getOutputColID(col);
}

ValueType Remote::getColType(const char *col) const
{
    return child->getColType(col);
}

double Remote::estCost() const
{
    return child->estCost() + COST_NET_XFER_BYTE * estTupleLength() * estCardinality();
}

double Remote::estCardinality() const
{
    return child->estCardinality();
}

double Remote::estTupleLength() const
{
    return child->estTupleLength();
}

double Remote::estColLength(const ColID cid) const
{
    return child->estColLength(cid);
}
