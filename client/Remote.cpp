#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "Remote.h"
#include "NLJoin.h"
#include "SeqScan.h"
#include "IndexScan.h"

using namespace op;


Remote::Remote(const NodeID n, boost::shared_ptr<Operator> c, const char *i)
    : Operator(n), child(c), ipAddress(i), tcpstream(), lineBuffer(new char[4096])
{
    for (ColID i = 0; i < child->numOutputCols(); i++) {
        selectedInputColIDs.push_back(i);
    }
}

Remote::Remote()
    : child(), ipAddress(), tcpstream(), lineBuffer(new char[4096])
{
}

Remote::~Remote()
{
}

RC Remote::Open()
{
    tcpstream.connect(ipAddress, boost::lexical_cast<std::string>(30000 + child->getNodeID()));
    if (tcpstream.fail()) {
        throw std::runtime_error("tcp::iostream.connect() failed");
    }

    boost::archive::binary_oarchive oa(tcpstream);
    oa.register_type(static_cast<op::NLJoin *>(NULL));
    oa.register_type(static_cast<op::SeqScan *>(NULL));
    oa.register_type(static_cast<op::IndexScan *>(NULL));
    oa.register_type(static_cast<op::Remote *>(NULL));

    oa << child;
    return 0;
}

RC Remote::GetNext(Tuple &tuple)
{
    if (tcpstream.eof()) {
        return -1;
    }
    tcpstream.getline(lineBuffer.get(), 4096);
    if (*lineBuffer.get() == '\0') {
        return -1;
    }

    char *c = lineBuffer.get();
    tuple.clear();
    while (true) {
        tuple.push_back(c);
        if (!(c = strchr(c + 1, '|'))) {
            break;
        }
        *c++ = 0;
    }

    return 0;
}

RC Remote::Close()
{
    tcpstream.close();
    return 0;
}

void Remote::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Remote@" << getNodeID();
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
