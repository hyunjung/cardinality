#include "NLJoin.h"

using namespace ca;


NLJoin::NLJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q, const int x, const char *idxJoinCol)
    : Join(n, l, r, q, x),
      idxJoinColID(NOT_INDEX_JOIN)
{
    if (idxJoinCol) {
        idxJoinColID = getInputColID(idxJoinCol);
    }
}

NLJoin::NLJoin()
    : Join(),
      idxJoinColID()
{
}

NLJoin::NLJoin(const NLJoin &x)
    : Join(x),
      idxJoinColID(x.idxJoinColID)
{
}

NLJoin::~NLJoin()
{
}

Operator::Ptr NLJoin::clone() const
{
    return Operator::Ptr(new NLJoin(*this));
}

RC NLJoin::Open(const char *, const uint32_t)
{
    state = RIGHT_OPEN;
    return leftChild->Open();
}

RC NLJoin::ReOpen(const char *, const uint32_t)
{
    throw std::runtime_error("NLJoin::ReOpen() called");
}

RC NLJoin::GetNext(Tuple &tuple)
{
    Tuple rightTuple;

    while (true) {
        if (state == RIGHT_OPEN) {
            if (leftChild->GetNext(leftTuple)) {
                return -1;
            }
            state = RIGHT_GETNEXT;
            if (idxJoinColID == NOT_INDEX_JOIN) {
                rightChild->Open();
            } else {
                rightChild->Open(leftTuple[idxJoinColID].first,
                                 leftTuple[idxJoinColID].second);
            }
        } else if (state == RIGHT_REOPEN) {
            if (leftChild->GetNext(leftTuple)) {
                rightChild->Close();
                return -1;
            }
            state = RIGHT_GETNEXT;
            if (idxJoinColID == NOT_INDEX_JOIN) {
                rightChild->ReOpen();
            } else {
                rightChild->ReOpen(leftTuple[idxJoinColID].first,
                                   leftTuple[idxJoinColID].second);
            }
        }

        while (!rightChild->GetNext(rightTuple)) {
            if (execFilter(leftTuple, rightTuple)) {
                execProject(leftTuple, rightTuple, tuple);
                return 0;
            }
        }

        state = RIGHT_REOPEN;
    }

    return 0;
}

RC NLJoin::Close()
{
    return leftChild->Close();
}

void NLJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NLJoin@" << getNodeID();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    leftChild->print(os, tab + 1);
    rightChild->print(os, tab + 1);
}

double NLJoin::estCost() const
{
    return leftChild->estCost() + leftChild->estCardinality() * rightChild->estCost();
}
