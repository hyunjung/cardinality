#include "client/Project.h"
#include <algorithm>
#include <set>


namespace cardinality {

Project::Project(const NodeID n)
    : Operator(n),
      selected_input_col_ids_()
{
}

Project::Project()
    : Operator(),
      selected_input_col_ids_()
{
}

Project::Project(const Project &x)
    : Operator(x),
      selected_input_col_ids_(x.selected_input_col_ids_)
{
}

Project::~Project()
{
}

ColID Project::numOutputCols() const
{
    return static_cast<ColID>(selected_input_col_ids_.size());
}

ColID Project::getOutputColID(const char *col) const
{
    return std::find(selected_input_col_ids_.begin(),
                     selected_input_col_ids_.end(),
                     getInputColID(col))
           - selected_input_col_ids_.begin();
}

void Project::initProject(const Query *q)
{
    std::set<std::string> selected_cols;

    for (int i = 0; i < q->nbOutputFields; ++i) {
        if (hasCol(q->outputFields[i])
            && selected_cols.count(std::string(q->outputFields[i])) == 0) {
            selected_cols.insert(std::string(q->outputFields[i]));
            selected_input_col_ids_.push_back(getInputColID(q->outputFields[i]));
        }
    }
    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && !hasCol(q->joinFields2[i])
            && selected_cols.count(std::string(q->joinFields1[i])) == 0) {
            selected_cols.insert(std::string(q->joinFields1[i]));
            selected_input_col_ids_.push_back(getInputColID(q->joinFields1[i]));
        } else if (hasCol(q->joinFields2[i]) && !hasCol(q->joinFields1[i])
                   && selected_cols.count(std::string(q->joinFields2[i])) == 0) {
            selected_cols.insert(std::string(q->joinFields2[i]));
            selected_input_col_ids_.push_back(getInputColID(q->joinFields2[i]));
        }
    }
}

}  // namespace cardinality
