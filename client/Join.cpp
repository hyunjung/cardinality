#include "client/Join.h"
#include <cstring>


namespace cardinality {

Join::Join(const NodeID n, Operator::Ptr l, Operator::Ptr r,
           const Query *q, const int x)
    : Operator(n),
      left_child_(l),
      right_child_(r),
      join_conds_()
{
    initProject(q);
    initFilter(q, x);
}

Join::Join()
    : Operator(),
      left_child_(),
      right_child_(),
      join_conds_()
{
}

Join::Join(const Join &x)
    : Operator(x),
      left_child_(x.left_child_->clone()),
      right_child_(x.right_child_->clone()),
      join_conds_(x.join_conds_)
{
}

Join::~Join()
{
}

void Join::initFilter(const Query *q, const int x)
{
    for (int i = 0; i < q->nbJoins; ++i) {
        if (i == x) {  // this condition is evaluated by IndexScan
            continue;
        }
        if (right_child_->hasCol(q->joinFields1[i])
            && left_child_->hasCol(q->joinFields2[i])) {
            join_conds_.push_back(
                boost::make_tuple(left_child_->getOutputColID(q->joinFields2[i]),
                                  right_child_->getOutputColID(q->joinFields1[i]),
                                  right_child_->getColType(q->joinFields1[i])));
        } else if (right_child_->hasCol(q->joinFields2[i])
                   && left_child_->hasCol(q->joinFields1[i])) {
            join_conds_.push_back(
                boost::make_tuple(left_child_->getOutputColID(q->joinFields1[i]),
                                  right_child_->getOutputColID(q->joinFields2[i]),
                                  right_child_->getColType(q->joinFields2[i])));
        }
    }
}

bool Join::execFilter(const Tuple &left_tuple, const Tuple &right_tuple) const
{
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        if (join_conds_[i].get<2>() == INT) {
            if (parseInt(left_tuple[join_conds_[i].get<0>()].first,
                         left_tuple[join_conds_[i].get<0>()].second)
                != parseInt(right_tuple[join_conds_[i].get<1>()].first,
                            right_tuple[join_conds_[i].get<1>()].second)) {
                return false;
            }
        } else {  // STRING
            if ((left_tuple[join_conds_[i].get<0>()].second
                 != right_tuple[join_conds_[i].get<1>()].second)
                || std::memcmp(left_tuple[join_conds_[i].get<0>()].first,
                               right_tuple[join_conds_[i].get<1>()].first,
                               right_tuple[join_conds_[i].get<1>()].second)) {
                return false;
            }
        }
    }

    return true;
}

void Join::execProject(const Tuple &left_tuple, const Tuple &right_tuple, Tuple &output_tuple) const
{
    output_tuple.clear();

    for (std::size_t i = 0; i < numOutputCols(); ++i) {
        if (selected_input_col_ids_[i] < static_cast<ColID>(left_child_->numOutputCols())) {
            output_tuple.push_back(
                left_tuple[selected_input_col_ids_[i]]);
        } else {
            output_tuple.push_back(
                right_tuple[selected_input_col_ids_[i] - left_child_->numOutputCols()]);
        }
    }
}

bool Join::hasCol(const char *col) const
{
    return right_child_->hasCol(col) || left_child_->hasCol(col);
}

ColID Join::getInputColID(const char *col) const
{
    if (right_child_->hasCol(col)) {
        return right_child_->getOutputColID(col) + left_child_->numOutputCols();
    } else {
        return left_child_->getOutputColID(col);
    }
}

ValueType Join::getColType(const char *col) const
{
    if (right_child_->hasCol(col)) {
        return right_child_->getColType(col);
    } else {
        return left_child_->getColType(col);
    }
}

double Join::estCardinality() const
{
    double card = left_child_->estCardinality()
                  * right_child_->estCardinality();

    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        if (join_conds_[i].get<1>() == 0) {
            card /= right_child_->estCardinality();
        } else {
            card *= SELECTIVITY_EQ;
        }
    }

    return card;
}

double Join::estTupleLength() const
{
    double length = 0;
    for (std::size_t i = 0; i < numOutputCols(); ++i) {
        length += estColLength(i);
    }

    return length;
}

double Join::estColLength(const ColID cid) const
{
    if (selected_input_col_ids_[cid] < static_cast<ColID>(left_child_->numOutputCols())) {
        return left_child_->estColLength(selected_input_col_ids_[cid]);
    } else {
        return right_child_->estColLength(
                   selected_input_col_ids_[cid] - left_child_->numOutputCols());
    }
}

}  // namespace cardinality
