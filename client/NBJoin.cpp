#include "NBJoin.h"

#ifndef NBJOIN_BUFSIZE
#define NBJOIN_BUFSIZE 65536
#endif

using namespace ca;


NBJoin::NBJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q)
    : Join(n, l, r, q),
      state_(), left_done_(), left_tuples_(), left_tuples_it_(), right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::NBJoin()
    : Join(),
      state_(), left_done_(), left_tuples_(), left_tuples_it_(), right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::NBJoin(const NBJoin &x)
    : Join(x),
      state_(), left_done_(), left_tuples_(), left_tuples_it_(), right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::~NBJoin()
{
}

Operator::Ptr NBJoin::clone() const
{
    return Operator::Ptr(new NBJoin(*this));
}

RC NBJoin::Open(const char *, const uint32_t)
{
    main_buffer_.reset(new char[NBJOIN_BUFSIZE]);
    state_ = RIGHT_OPEN;
    return left_child_->Open();
}

RC NBJoin::ReOpen(const char *, const uint32_t)
{
    throw std::runtime_error("NBJoin::ReOpen() called");
}

RC NBJoin::GetNext(Tuple &tuple)
{
    while (true) {
        switch (state_) {
        case RIGHT_OPEN:
        case RIGHT_REOPEN: {
            Tuple left_tuple;
            for (char *pos = main_buffer_.get();
                 pos - main_buffer_.get() < NBJOIN_BUFSIZE - 512
                 && !(left_done_ = left_child_->GetNext(left_tuple)); ) {
                for (size_t i = 0; i < left_tuple.size(); ++i) {
                    uint32_t len = left_tuple[i].second;
                    if (pos + len < main_buffer_.get() + NBJOIN_BUFSIZE) {
                        std::memcpy(pos, left_tuple[i].first, len);
                        pos[len] = '\0';
                        left_tuple[i].first = pos;
                        pos += len + 1;
                    } else { // main_buffer_ doesn't have enough space
                        int overflow_len = len + 1;
                        for (size_t j = i + 1; j < left_tuple.size(); ++j) {
                            overflow_len += left_tuple[j].second + 1;
                        }
                        overflow_buffer_.reset(new char[overflow_len]);
                        pos = overflow_buffer_.get();
                        for (; i < left_tuple.size(); ++i) {
                            uint32_t len = left_tuple[i].second;
                            std::memcpy(pos, left_tuple[i].first, len);
                            pos[len] = '\0';
                            left_tuple[i].first = pos;
                            pos += len + 1;
                        }
                        pos = main_buffer_.get() + NBJOIN_BUFSIZE;
                    }
                }
                left_tuples_.push_back(left_tuple);
            }

            if (left_tuples_.empty()) {
                if (state_ == RIGHT_REOPEN) {
                    right_child_->Close();
                }
                return -1;
            }

            if (state_ == RIGHT_OPEN) {
                right_child_->Open();
            } else { // RIGHT_REOPEN
                right_child_->ReOpen();
            }
            state_ = RIGHT_GETNEXT;
        }

        case RIGHT_GETNEXT:
            if (right_child_->GetNext(right_tuple_)) {
                left_tuples_.clear();
                if (left_done_) {
                    right_child_->Close();
                    return -1;
                }
                state_ = RIGHT_REOPEN;
                break;
            }
            left_tuples_it_ = left_tuples_.begin();
            state_ = RIGHT_SWEEPBUFFER;

        case RIGHT_SWEEPBUFFER:
            for (; left_tuples_it_ < left_tuples_.end(); ++left_tuples_it_) {
                if (execFilter(*left_tuples_it_, right_tuple_)) {
                    execProject(*left_tuples_it_++, right_tuple_, tuple);
                    return 0;
                }
            }
            state_ = RIGHT_GETNEXT;
            break;
        }
    }

    return 0;
}

RC NBJoin::Close()
{
    main_buffer_.reset();
    overflow_buffer_.reset();
    return left_child_->Close();
}

void NBJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NBJoin@" << getNodeID();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    left_child_->print(os, tab + 1);
    right_child_->print(os, tab + 1);
}

double NBJoin::estCost() const
{
    return left_child_->estCost()
           + std::ceil(left_child_->estCardinality() * left_child_->estTupleLength() / NBJOIN_BUFSIZE)
             * right_child_->estCost();
}
