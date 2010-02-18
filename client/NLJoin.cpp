#include "NLJoin.h"

using namespace op;


NLJoin::NLJoin(const NodeID n, Operator::Ptr l, Scan::Ptr r,
               const Query *q, const int x, const char *idxJoinCol)
    : Join(n, l, r, q, x), idxJoinColID(NOT_INDEX_JOIN)
{
    if (idxJoinCol) {
        idxJoinColID = getInputColID(idxJoinCol);
    }
}

NLJoin::NLJoin()
    : idxJoinColID(NOT_INDEX_JOIN)
{
}

NLJoin::~NLJoin()
{
}

RC NLJoin::Open(const char *, const uint32_t)
{
    state = RIGHT_OPEN;
    return leftChild->Open();
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
        } else if (state == RIGHT_RESCAN) {
            if (leftChild->GetNext(leftTuple)) {
                rightChild->Close();
                return -1;
            }
            state = RIGHT_GETNEXT;
            if (idxJoinColID == NOT_INDEX_JOIN) {
                rightChild->ReScan();
            } else {
                rightChild->ReScan(leftTuple[idxJoinColID].first,
                                   leftTuple[idxJoinColID].second);
            }
        }

        while (!rightChild->GetNext(rightTuple)) {
            if (execFilter(leftTuple, rightTuple)) {
                execProject(leftTuple, rightTuple, tuple);
                return 0;
            }
        }

        state = RIGHT_RESCAN;
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
    os << " [" << selectedInputColIDs.size() << "] ";
    os << std::endl;

    leftChild->print(os, tab + 1);
    rightChild->print(os, tab + 1);
}
