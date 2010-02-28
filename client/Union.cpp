#include "Union.h"


using namespace ca;


Union::Union(const NodeID n, Operator::Ptr c)
    : Operator(n), child(c)
{
    for (size_t i = 0; i < child->numOutputCols(); ++i) {
        selectedInputColIDs.push_back(i);
    }
}

Union::Union()
    : child()
{
}

Union::~Union()
{
}

RC Union::Open(const char *, const uint32_t)
{
    return 0;
}

RC Union::GetNext(Tuple &tuple)
{
    return 0;
}

RC Union::Close()
{
    return 0;
}

void Union::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Union@" << getNodeID();
    os << " cost=" << estCost();
    os << std::endl;

    child->print(os, tab + 1);
}

bool Union::hasCol(const char *col) const
{
    return child->hasCol(col);
}

ColID Union::getInputColID(const char *col) const
{
    return child->getOutputColID(col);
}

ValueType Union::getColType(const char *col) const
{
    return child->getColType(col);
}

double Union::estCost() const
{
    return child->estCost();
}

double Union::estCardinality() const
{
    return child->estCardinality();
}

double Union::estTupleLength() const
{
    return child->estTupleLength();
}

double Union::estColLength(const ColID cid) const
{
    return child->estColLength(cid);
}
