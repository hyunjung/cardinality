#include "Join.h"

using namespace ca;


Join::Join(const NodeID n, Operator::Ptr l, Operator::Ptr r,
           const Query *q, const int x)
    : Operator(n), leftChild(l), rightChild(r), joinConds()
{
    initProject(q);
    initFilter(q, x);
}

Join::Join()
    : leftChild(), rightChild(), joinConds()
{
}

Join::~Join()
{
}

void Join::initFilter(const Query *q, const int x)
{
    for (int i = 0; i < q->nbJoins; ++i) {
        if (i == x) { // this condition is evaluated by IndexScan
            continue;
        }
        if (rightChild->hasCol(q->joinFields1[i]) && leftChild->hasCol(q->joinFields2[i])) {
            joinConds.push_back(boost::make_tuple(leftChild->getOutputColID(q->joinFields2[i]),
                                                  rightChild->getOutputColID(q->joinFields1[i]),
                                                  rightChild->getColType(q->joinFields1[i])));
        } else if (rightChild->hasCol(q->joinFields2[i]) && leftChild->hasCol(q->joinFields1[i])) {
            joinConds.push_back(boost::make_tuple(leftChild->getOutputColID(q->joinFields1[i]),
                                                  rightChild->getOutputColID(q->joinFields2[i]),
                                                  rightChild->getColType(q->joinFields2[i])));
        }
    }
}

bool Join::execFilter(const Tuple &leftTuple, const Tuple &rightTuple) const
{
    for (size_t i = 0; i < joinConds.size(); ++i) {
        if (joinConds[i].get<2>() == INT) {
            if (parseInt(leftTuple[joinConds[i].get<0>()].first,
                         leftTuple[joinConds[i].get<0>()].second)
                != parseInt(rightTuple[joinConds[i].get<1>()].first,
                            rightTuple[joinConds[i].get<1>()].second)) {
                return false;
            }
        } else { // STRING
            if ((leftTuple[joinConds[i].get<0>()].second
                 != rightTuple[joinConds[i].get<1>()].second)
                || std::memcmp(leftTuple[joinConds[i].get<0>()].first,
                               rightTuple[joinConds[i].get<1>()].first,
                               rightTuple[joinConds[i].get<1>()].second)) {
                return false;
            }
        }
    }

    return true;
}

void Join::execProject(const Tuple &leftTuple, const Tuple &rightTuple, Tuple &outputTuple) const
{
    outputTuple.clear();

    for (size_t i = 0; i < numOutputCols(); ++i) {
        if (selectedInputColIDs[i] < static_cast<ColID>(leftChild->numOutputCols())) {
            outputTuple.push_back(leftTuple[selectedInputColIDs[i]]);
        } else {
            outputTuple.push_back(rightTuple[selectedInputColIDs[i] - leftChild->numOutputCols()]);
        }
    }
}

bool Join::hasCol(const char *col) const
{
    return rightChild->hasCol(col) || leftChild->hasCol(col);
}

ColID Join::getInputColID(const char *col) const
{
    if (rightChild->hasCol(col)) {
        return rightChild->getOutputColID(col) + leftChild->numOutputCols();
    } else {
        return leftChild->getOutputColID(col);
    }
}

ValueType Join::getColType(const char *col) const
{
    if (rightChild->hasCol(col)) {
        return rightChild->getColType(col);
    } else {
        return leftChild->getColType(col);
    }
}

double Join::estCardinality() const
{
    double card = leftChild->estCardinality() * rightChild->estCardinality();

    for (size_t i = 0; i < joinConds.size(); ++i) {
        if (joinConds[i].get<1>() == 0) {
            card /= rightChild->estCardinality();
        } else {
            card *= SELECTIVITY_EQ;
        }
    }

    return card;
}

double Join::estTupleLength() const
{
    double length = 0;
    for (size_t i = 0; i < numOutputCols(); ++i) {
        length += estColLength(i);
    }

    return length;
}

double Join::estColLength(const ColID cid) const
{
    if (selectedInputColIDs[cid] < static_cast<ColID>(leftChild->numOutputCols())) {
        return leftChild->estColLength(selectedInputColIDs[cid]);
    } else {
        return rightChild->estColLength(selectedInputColIDs[cid] - leftChild->numOutputCols());
    }
}
