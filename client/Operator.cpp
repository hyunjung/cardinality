#include <algorithm>
#include <set>
#include <boost/spirit/include/qi.hpp>
#include "Operator.h"

using namespace ca;


Operator::Operator(const NodeID n)
    : node_id_(n), selected_input_col_ids_()
{
}

Operator::Operator()
    : node_id_(), selected_input_col_ids_()
{
}

Operator::Operator(const Operator &x)
    : node_id_(x.node_id_), selected_input_col_ids_(x.selected_input_col_ids_)
{
}

Operator::~Operator()
{
}

NodeID Operator::getNodeID() const
{
    return node_id_;
}

size_t Operator::numOutputCols() const
{
    return selected_input_col_ids_.size();
}

ColID Operator::getOutputColID(const char *col) const
{
    return std::find(selected_input_col_ids_.begin(), selected_input_col_ids_.end(), getInputColID(col))
           - selected_input_col_ids_.begin();
}

void Operator::initProject(const Query *q)
{
    std::set<std::string> selectedCols;

    for (int i = 0; i < q->nbOutputFields; ++i) {
        if (hasCol(q->outputFields[i])
            && selectedCols.count(std::string(q->outputFields[i])) == 0) {
            selectedCols.insert(std::string(q->outputFields[i]));
            selected_input_col_ids_.push_back(getInputColID(q->outputFields[i]));
        }
    }
    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && !hasCol(q->joinFields2[i])
            && selectedCols.count(std::string(q->joinFields1[i])) == 0) {
            selectedCols.insert(std::string(q->joinFields1[i]));
            selected_input_col_ids_.push_back(getInputColID(q->joinFields1[i]));
        } else if (hasCol(q->joinFields2[i]) && !hasCol(q->joinFields1[i])
                   && selectedCols.count(std::string(q->joinFields2[i])) == 0) {
            selectedCols.insert(std::string(q->joinFields2[i]));
            selected_input_col_ids_.push_back(getInputColID(q->joinFields2[i]));
        }
    }
}

uint32_t Operator::parseInt(const char *str, const uint32_t len)
{
    uint32_t intVal = 0;
    boost::spirit::qi::parse(str, str + len, boost::spirit::uint_, intVal);
    return intVal;
}
