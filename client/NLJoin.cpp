#include "NLJoin.h"

using namespace op;


NLJoin::NLJoin(const NodeID n, boost::shared_ptr<Operator> l, boost::shared_ptr<Scan> r,
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

RC NLJoin::Open(const char *)
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
            rightChild->Open((idxJoinColID == NOT_INDEX_JOIN) ? NULL
                                                              : leftTuple[idxJoinColID]);
        } else if (state == RIGHT_RESCAN) {
            if (leftChild->GetNext(leftTuple)) {
                rightChild->Close();
                return -1;
            }
            state = RIGHT_GETNEXT;
            rightChild->ReScan((idxJoinColID == NOT_INDEX_JOIN) ? NULL
                                                                : leftTuple[idxJoinColID]);
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
    os << "NLJoin@" << getNodeID() << "    ";
    printOutputCols(os);
    os << std::endl;

    leftChild->print(os, tab + 1);
    rightChild->print(os, tab + 1);
}
