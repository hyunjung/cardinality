#include "NLJoin.h"

using namespace op;


NLJoin::NLJoin(const NodeID n, boost::shared_ptr<Operator> l, boost::shared_ptr<Scan> r, const Query *q)
    : Join(n, l, r, q)
{
}

NLJoin::NLJoin()
{
}

NLJoin::~NLJoin()
{
}

RC NLJoin::open()
{
    reloadLeft = true;
    return leftChild->open();
}

RC NLJoin::getNext(Tuple &tuple)
{
    Tuple rightTuple;

    while (true) {
        if (reloadLeft) {
            leftTuple.clear();
            if (leftChild->getNext(leftTuple)) {
                return -1;
            }
            reloadLeft = false;
            rightChild->open();
        }

        while (!rightChild->getNext(rightTuple)) {
            if (execFilter(leftTuple, rightTuple)) {
                execProject(leftTuple, rightTuple, tuple);
                return 0;
            }
        }

        rightChild->close();
        reloadLeft = true;
    }

    return 0;
}

RC NLJoin::close()
{
    return leftChild->close();
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
