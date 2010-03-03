#include <algorithm>
#include <set>
#include <boost/spirit/include/qi.hpp>
#include "Operator.h"

using namespace ca;


Operator::Operator(const NodeID n)
    : nodeID(n), selectedInputColIDs()
{
}

Operator::Operator()
    : nodeID(), selectedInputColIDs()
{
}

Operator::Operator(const Operator &x)
    : nodeID(x.nodeID), selectedInputColIDs(x.selectedInputColIDs)
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

uint32_t Operator::parseInt(const char *str, const uint32_t len)
{
    uint32_t intVal = 0;
    boost::spirit::qi::parse(str, str + len, boost::spirit::uint_, intVal);
    return intVal;
}
