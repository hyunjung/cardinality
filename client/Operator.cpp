#include <algorithm>
#include "Operator.h"
#include "../include/client.h"

using namespace op;


Operator::Operator(): selectedInputColIDs(), outputColToColID()
{
}

Operator::~Operator()
{
}

size_t Operator::numOutputCols() const
{
    return selectedInputColIDs.size();
}

int Operator::getOutputColID(const char *col) const
{
    return outputColToColID.find(std::string(col))->second;
}

void Operator::initProject(const Query *q)
{
    for (int i = 0; i < q->nbOutputFields; ++i) {
        if (hasCol(q->outputFields[i])
            && outputColToColID.count(std::string(q->outputFields[i])) == 0) {
            outputColToColID[std::string(q->outputFields[i])] = numOutputCols();
            selectedInputColIDs.push_back(getInputColID(q->outputFields[i]));
        }
    }
    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && !hasCol(q->joinFields2[i])
            && outputColToColID.count(std::string(q->joinFields1[i])) == 0) {
            outputColToColID[std::string(q->joinFields1[i])] = numOutputCols();
            selectedInputColIDs.push_back(getInputColID(q->joinFields1[i]));
        } else if (hasCol(q->joinFields2[i]) && !hasCol(q->joinFields1[i])
                   && outputColToColID.count(std::string(q->joinFields2[i])) == 0) {
            outputColToColID[std::string(q->joinFields2[i])] = numOutputCols();
            selectedInputColIDs.push_back(getInputColID(q->joinFields2[i]));
        }
    }
}

void Operator::printOutputCols(std::ostream &os) const
{
    std::map<std::string, int>::const_iterator it;
    for (it = outputColToColID.begin(); it != outputColToColID.end(); ++it) {
        os << it->first << " ";
    }
}
