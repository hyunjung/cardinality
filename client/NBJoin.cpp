// Copyright (c) 2010, Hyunjung Park
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Stanford University nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "client/NBJoin.h"
#include <cstring>
#include <stdexcept>  // std::runtime_error
#include <algorithm>  // std::max, std::min


namespace cardinality {

static const int NBJOIN_BUFSIZE = 262144;

NBJoin::NBJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q)
    : Join(n, l, r, q),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::NBJoin(google::protobuf::io::CodedInputStream *input)
    : Join(input),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      main_buffer_(), overflow_buffer_()
{
    Deserialize(input);
}

NBJoin::NBJoin(const NBJoin &x)
    : Join(x),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::~NBJoin()
{
}

Operator::Ptr NBJoin::clone() const
{
    return boost::make_shared<NBJoin>(*this);
}

void NBJoin::Open(const char *, const uint32_t)
{
    main_buffer_.reset(new char[NBJOIN_BUFSIZE]);
    left_tuples_.reset(new multimap());

    state_ = STATE_OPEN;
    left_tuple_.reserve(left_child_->numOutputCols());
    right_tuple_.reserve(right_child_->numOutputCols());
    left_child_->Open();
}

void NBJoin::ReOpen(const char *, const uint32_t)
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

bool NBJoin::GetNext(Tuple &tuple)
{
    for (;;) {
        switch (state_) {
        case STATE_OPEN:
        case STATE_REOPEN: {
            for (char *pos = main_buffer_.get();
                 pos - main_buffer_.get() < NBJOIN_BUFSIZE - 512
                 && !(left_done_ = left_child_->GetNext(left_tuple_)); ) {
                for (std::size_t i = 0; i < left_tuple_.size(); ++i) {
                    uint32_t len = left_tuple_[i].second;
                    if (pos + len < main_buffer_.get() + NBJOIN_BUFSIZE) {
                        std::memcpy(pos, left_tuple_[i].first, len);
                        pos[len] = '\0';
                        left_tuple_[i].first = pos;
                        pos += len + 1;
                    } else {  // main_buffer_ doesn't have enough space
                        int overflow_len = len + 1;
                        for (std::size_t j = i + 1;
                             j < left_tuple_.size(); ++j) {
                            overflow_len += left_tuple_[j].second + 1;
                        }
                        overflow_buffer_.reset(new char[overflow_len]);
                        pos = overflow_buffer_.get();
                        for (; i < left_tuple_.size(); ++i) {
                            uint32_t len = left_tuple_[i].second;
                            std::memcpy(pos, left_tuple_[i].first, len);
                            pos[len] = '\0';
                            left_tuple_[i].first = pos;
                            pos += len + 1;
                        }
                        pos = main_buffer_.get() + NBJOIN_BUFSIZE;
                    }
                }

                if (join_conds_.empty()) {  // cross product
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        0, left_tuple_));
                } else if (join_conds_[0].get<2>()) {  // STRING
                    ColID cid = join_conds_[0].get<0>();
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        hashString(left_tuple_[cid].first,
                                   left_tuple_[cid].second),
                        left_tuple_));
                } else {  // INT
                    ColID cid = join_conds_[0].get<0>();
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        parseInt(left_tuple_[cid].first,
                                 left_tuple_[cid].second),
                        left_tuple_));
                }
            }

            if (left_tuples_->empty()) {
                return true;
            }

            if (state_ == STATE_OPEN) {
                right_child_->Open();
            } else {  // STATE_REOPEN
                right_child_->ReOpen();
            }
            state_ = STATE_GETNEXT;
        }

        case STATE_GETNEXT:
            if (right_child_->GetNext(right_tuple_)) {
                left_tuples_->clear();
                if (left_done_) {
                    return true;
                }
                state_ = STATE_REOPEN;
                break;
            }

            if (join_conds_.empty()) {  // cross product
                left_tuples_it_ = left_tuples_->begin();
                left_tuples_end_ = left_tuples_->end();
            } else if (join_conds_[0].get<2>()) {  // STRING
                ColID cid = join_conds_[0].get<1>();
                std::pair<multimap::const_iterator,
                          multimap::const_iterator> range
                    = left_tuples_->equal_range(
                          hashString(right_tuple_[cid].first,
                                     right_tuple_[cid].second));
                left_tuples_it_ = range.first;
                left_tuples_end_ = range.second;
            } else {  // INT
                ColID cid = join_conds_[0].get<1>();
                std::pair<multimap::const_iterator,
                          multimap::const_iterator> range
                    = left_tuples_->equal_range(
                          parseInt(right_tuple_[cid].first,
                                   right_tuple_[cid].second));
                left_tuples_it_ = range.first;
                left_tuples_end_ = range.second;
            }
            state_ = STATE_SWEEPBUFFER;

        case STATE_SWEEPBUFFER:
            for (; left_tuples_it_ != left_tuples_end_; ++left_tuples_it_) {
                if (execFilter(left_tuples_it_->second, right_tuple_)) {
                    execProject((left_tuples_it_++)->second,
                                right_tuple_, tuple);
                    return false;
                }
            }
            state_ = STATE_GETNEXT;
            break;
        }
    }

    return false;
}

void NBJoin::Close()
{
    left_tuples_.reset();
    main_buffer_.reset();
    overflow_buffer_.reset();

    if (state_ != STATE_OPEN) {
        right_child_->Close();
    }
    left_child_->Close();
}

uint8_t *NBJoin::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(TAG_NBJOIN, target);

    target = Join::SerializeToArray(target);

    return target;
}

int NBJoin::ByteSize() const
{
    int total_size = 1 + Join::ByteSize();

    return total_size;
}

void NBJoin::Deserialize(google::protobuf::io::CodedInputStream *input)
{
}

#ifdef PRINT_PLAN
void NBJoin::print(std::ostream &os, const int tab, const double) const
{
    os << std::string(4 * tab, ' ');
    os << "NBJoin@" << node_id();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    left_child_->print(os, tab + 1);
    right_child_->print(os, tab + 1);
}
#endif

double NBJoin::estCost(const double) const
{
    return left_child_->estCost()
           + std::max(1.0, left_child_->estCardinality()
                           * left_child_->estTupleLength() / NBJOIN_BUFSIZE)
             * right_child_->estCost();
}

uint64_t NBJoin::hashString(const char *str, const uint32_t len)
{
    uint64_t hash = 0;
    std::memcpy(&hash, str, std::min(len, 8u));
    return hash;
}

}  // namespace cardinality
