#include <algorithm>
#include <set>
#include "Operator.h"

using namespace op;


Operator::Operator(const NodeID n)
    : nodeID(n), selectedInputColIDs()
{
}

Operator::Operator()
    : nodeID(0), selectedInputColIDs()
{
}

Operator::~Operator()
{
}

NodeID Operator::getNodeID() const
{
    return nodeID;
}

size_t Operator::numOutputCols() const
{
    return selectedInputColIDs.size();
}

ColID Operator::getOutputColID(const char *col) const
{
    return std::find(selectedInputColIDs.begin(), selectedInputColIDs.end(), getInputColID(col))
           - selectedInputColIDs.begin();
}

void Operator::initProject(const Query *q)
{
    std::set<std::string> selectedCols;

    for (int i = 0; i < q->nbOutputFields; ++i) {
        if (hasCol(q->outputFields[i])
            && selectedCols.count(std::string(q->outputFields[i])) == 0) {
            selectedCols.insert(std::string(q->outputFields[i]));
            selectedInputColIDs.push_back(getInputColID(q->outputFields[i]));
        }
    }
    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && !hasCol(q->joinFields2[i])
            && selectedCols.count(std::string(q->joinFields1[i])) == 0) {
            selectedCols.insert(std::string(q->joinFields1[i]));
            selectedInputColIDs.push_back(getInputColID(q->joinFields1[i]));
        } else if (hasCol(q->joinFields2[i]) && !hasCol(q->joinFields1[i])
                   && selectedCols.count(std::string(q->joinFields2[i])) == 0) {
            selectedCols.insert(std::string(q->joinFields2[i]));
            selectedInputColIDs.push_back(getInputColID(q->joinFields2[i]));
        }
    }
}
