#include "client/NLJoin.h"


namespace cardinality {

NLJoin::NLJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q, const int x, const char *idxJoinCol)
    : Join(n, l, r, q, x),
      index_join_col_id_(NOT_INDEX_JOIN)
{
    if (idxJoinCol) {
        index_join_col_id_ = getInputColID(idxJoinCol);
    }
}

NLJoin::NLJoin()
    : Join(),
      index_join_col_id_()
{
}

NLJoin::NLJoin(const NLJoin &x)
    : Join(x),
      index_join_col_id_(x.index_join_col_id_)
{
}

NLJoin::~NLJoin()
{
}

Operator::Ptr NLJoin::clone() const
{
    return Operator::Ptr(new NLJoin(*this));
}

void NLJoin::Open(const char *, const uint32_t)
{
    state_ = RIGHT_OPEN;
    left_child_->Open();
}

void NLJoin::ReOpen(const char *, const uint32_t)
{
    throw std::runtime_error("NLJoin::ReOpen() called");
}

bool NLJoin::GetNext(Tuple &tuple)
{
    Tuple right_tuple;

    for (;;) {
        if (state_ == RIGHT_OPEN) {
            if (left_child_->GetNext(left_tuple_)) {
                return true;
            }
            state_ = RIGHT_GETNEXT;
            if (index_join_col_id_ == NOT_INDEX_JOIN) {
                right_child_->Open();
            } else {
                right_child_->Open(left_tuple_[index_join_col_id_].first,
                                   left_tuple_[index_join_col_id_].second);
            }
        } else if (state_ == RIGHT_REOPEN) {
            if (left_child_->GetNext(left_tuple_)) {
                right_child_->Close();
                return true;
            }
            state_ = RIGHT_GETNEXT;
            if (index_join_col_id_ == NOT_INDEX_JOIN) {
                right_child_->ReOpen();
            } else {
                right_child_->ReOpen(left_tuple_[index_join_col_id_].first,
                                     left_tuple_[index_join_col_id_].second);
            }
        }

        while (!right_child_->GetNext(right_tuple)) {
            if (execFilter(left_tuple_, right_tuple)) {
                execProject(left_tuple_, right_tuple, tuple);
                return false;
            }
        }

        state_ = RIGHT_REOPEN;
    }

    return false;
}

void NLJoin::Close()
{
    left_child_->Close();
}

void NLJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NLJoin@" << node_id();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    left_child_->print(os, tab + 1);
    right_child_->print(os, tab + 1);
}

double NLJoin::estCost(const double) const
{
    double left_cardinality = left_child_->estCardinality();
    return left_child_->estCost()
           + left_cardinality * right_child_->estCost(left_cardinality);
}

}  // namespace cardinality
