#include "Join.h"

using namespace op;


Join::Join(const NodeID n, boost::shared_ptr<Operator> l, boost::shared_ptr<Scan> r, const Query *q)
    : Operator(n), leftChild(l), rightChild(r), joinConds(), reloadLeft(true), leftTuple()
{
    initProject(q);
    initFilter(q);
}

Join::Join()
    : leftChild(), rightChild(), joinConds(), reloadLeft(true), leftTuple()
{
}

Join::~Join()
{
}

void Join::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbJoins; ++i) {
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

bool Join::execFilter(Tuple &leftTuple, Tuple &rightTuple) const
{
    for (size_t i = 0; i < joinConds.size(); ++i) {
        if ((joinConds[i].get<2>() == INT
             && atoi(leftTuple[joinConds[i].get<0>()]) != atoi(rightTuple[joinConds[i].get<1>()]))
            || (joinConds[i].get<2>() == STRING
                && strcmp(leftTuple[joinConds[i].get<0>()], rightTuple[joinConds[i].get<1>()]) != 0)) {
            return false;
        }
    }

    return true;
}

void Join::execProject(Tuple &leftTuple, Tuple &rightTuple, Tuple &outputTuple) const
{
    outputTuple.clear();

    for (size_t i = 0; i < numOutputCols(); ++i) {
        if (selectedInputColIDs[i] < leftChild->numOutputCols()) {
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
