#include "Union.h"


using namespace ca;


Union::Union(const NodeID n, std::vector<Operator::Ptr> c)
    : Operator(n),
      children_(c), it_(), done_()
{
    for (size_t i = 0; i < children_[0]->numOutputCols(); ++i) {
        selected_input_col_ids_.push_back(i);
    }
}

Union::Union()
    : Operator(),
      children_(), it_(), done_()
{
}

Union::Union(const Union &x)
    : Operator(x),
      children_(), it_(), done_()
{
    children_.reserve((x.children_.size() + 1) / 2);
    for (std::vector<Operator::Ptr>::const_iterator it = x.children_.begin();
         it < x.children_.end(); ++it) {
        children_.push_back((*it)->clone());
    }
}

Union::~Union()
{
}

Operator::Ptr Union::clone() const
{
    return Operator::Ptr(new Union(*this));
}

RC Union::Open(const char *left_ptr, const uint32_t left_len)
{
    done_.resize(children_.size());
    for (size_t i = 0; i < children_.size(); ++i) {
        children_[i]->Open(left_ptr, left_len);
        done_[i] = false;
    }
    it_ = 0;

    return 0;
}

RC Union::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    for (size_t i = 0; i < children_.size(); ++i) {
        children_[i]->ReOpen(left_ptr, left_len);
        done_[i] = false;
    }
    it_ = 0;

    return 0;
}

RC Union::GetNext(Tuple &tuple)
{
    for (; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return 0;
            }
        }
    }

    for (it_ = 0; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return 0;
            }
        }
    }

    return -1;
}

RC Union::Close()
{
    for (size_t i = 0; i < children_.size(); ++i) {
        children_[i]->Close();
    }
    return 0;
}

void Union::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Union@" << getNodeID();
    os << " cost=" << estCost();
    os << std::endl;

    for (size_t i = 0; i < children_.size(); ++i) {
        children_[i]->print(os, tab + 1);
    }
}

bool Union::hasCol(const char *col) const
{
    return children_[0]->hasCol(col);
}

ColID Union::getInputColID(const char *col) const
{
    return children_[0]->getOutputColID(col);
}

ValueType Union::getColType(const char *col) const
{
    return children_[0]->getColType(col);
}

double Union::estCost() const
{
    double cost = 0;
    for (size_t i = 0; i < children_.size(); ++i) {
        cost += children_[i]->estCost();
    }

    return cost;
}

double Union::estCardinality() const
{
    double card = 0;
    for (size_t i = 0; i < children_.size(); ++i) {
        card += children_[i]->estCost();
    }

    return card;
}

double Union::estTupleLength() const
{
    return children_[0]->estTupleLength();
}

double Union::estColLength(const ColID cid) const
{
    return children_[0]->estColLength(cid);
}
