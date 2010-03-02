#include "Union.h"


using namespace ca;


Union::Union(const NodeID n, std::vector<Operator::Ptr> c)
    : Operator(n), children(c), j(), done()
{
    for (size_t i = 0; i < children[0]->numOutputCols(); ++i) {
        selectedInputColIDs.push_back(i);
    }
}

Union::Union()
    : children(), j(), done()
{
}

Union::~Union()
{
}

RC Union::Open(const char *leftPtr, const uint32_t leftLen)
{
    done.resize(children.size());
    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->Open(leftPtr, leftLen);
        done[i] = false;
    }
    j = 0;

    return 0;
}

RC Union::ReOpen(const char *leftPtr, const uint32_t leftLen)
{
    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->ReOpen(leftPtr, leftLen);
        done[i] = false;
    }
    j = 0;

    return 0;
}

RC Union::GetNext(Tuple &tuple)
{
    for (; j < children.size(); ++j) {
        if (!done[j]) {
            if (children[j]->GetNext(tuple)) {
                done[j] = true;
            } else {
                ++j;
                return 0;
            }
        }
    }

    for (j = 0; j < children.size(); ++j) {
        if (!done[j]) {
            if (children[j]->GetNext(tuple)) {
                done[j] = true;
            } else {
                ++j;
                return 0;
            }
        }
    }

    return -1;
}

RC Union::Close()
{
    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->Close();
    }
    return 0;
}

void Union::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Union@" << getNodeID();
    os << " cost=" << estCost();
    os << std::endl;

    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->print(os, tab + 1);
    }
}

bool Union::hasCol(const char *col) const
{
    return children[0]->hasCol(col);
}

ColID Union::getInputColID(const char *col) const
{
    return children[0]->getOutputColID(col);
}

ValueType Union::getColType(const char *col) const
{
    return children[0]->getColType(col);
}

double Union::estCost() const
{
    double cost = 0;
    for (size_t i = 0; i < children.size(); ++i) {
        cost += children[i]->estCost();
    }

    return cost;
}

double Union::estCardinality() const
{
    double card = 0;
    for (size_t i = 0; i < children.size(); ++i) {
        card += children[i]->estCost();
    }

    return card;
}

double Union::estTupleLength() const
{
    return children[0]->estTupleLength();
}

double Union::estColLength(const ColID cid) const
{
    return children[0]->estColLength(cid);
}
