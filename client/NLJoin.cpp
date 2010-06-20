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

#include "client/NLJoin.h"
#include <stdexcept>  // std::runtime_error


namespace cardinality {

NLJoin::NLJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q, const int x, const char *idxJoinCol)
    : Join(n, l, r, q, x),
      index_join_col_id_(NOT_INDEX_JOIN),
      state_()
{
    if (idxJoinCol) {
        index_join_col_id_ = getInputColID(idxJoinCol);
    }
}

NLJoin::NLJoin(google::protobuf::io::CodedInputStream *input)
    : Join(input),
      index_join_col_id_(),
      state_()
{
    Deserialize(input);
}

NLJoin::NLJoin(const NLJoin &x)
    : Join(x),
      index_join_col_id_(x.index_join_col_id_),
      state_()
{
}

NLJoin::~NLJoin()
{
}

Operator::Ptr NLJoin::clone() const
{
    return boost::make_shared<NLJoin>(*this);
}

void NLJoin::Open(const Chunk *)
{
    state_ = STATE_OPEN;
    left_tuple_.reserve(left_child_->numOutputCols());
    right_tuple_.reserve(right_child_->numOutputCols());
    left_child_->Open();
}

void NLJoin::ReOpen(const Chunk *)
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

bool NLJoin::GetNext(Tuple &tuple)
{
    for (;;) {
        if (state_ == STATE_OPEN) {
            if (left_child_->GetNext(left_tuple_)) {
                return true;
            }
            state_ = STATE_GETNEXT;
            if (index_join_col_id_ == NOT_INDEX_JOIN) {
                right_child_->Open();
            } else {
                right_child_->Open(&left_tuple_[index_join_col_id_]);
            }
        } else if (state_ == STATE_REOPEN) {
            if (left_child_->GetNext(left_tuple_)) {
                return true;
            }
            state_ = STATE_GETNEXT;
            if (index_join_col_id_ == NOT_INDEX_JOIN) {
                right_child_->ReOpen();
            } else {
                right_child_->ReOpen(&left_tuple_[index_join_col_id_]);
            }
        }

        while (!right_child_->GetNext(right_tuple_)) {
            if (execFilter(left_tuple_, right_tuple_)) {
                execProject(left_tuple_, right_tuple_, tuple);
                return false;
            }
        }

        state_ = STATE_REOPEN;
    }

    return false;
}

void NLJoin::Close()
{
    if (state_ != STATE_OPEN) {
        right_child_->Close();
    }
    left_child_->Close();
}

uint8_t *NLJoin::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(TAG_NLJOIN, target);

    target = Join::SerializeToArray(target);

    target = CodedOutputStream::WriteLittleEndian32ToArray(
                 index_join_col_id_, target);

    return target;
}

int NLJoin::ByteSize() const
{
    int total_size = 1 + Join::ByteSize();

    total_size += 4;

    return total_size;
}

void NLJoin::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    uint32_t col_id;
    input->ReadLittleEndian32(&col_id);
    index_join_col_id_ = static_cast<ColID>(col_id);
}

void NLJoin::print(std::ostream &os, const int tab, const double) const
{
    os << std::string(4 * tab, ' ');
    os << "NLJoin@" << node_id();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleSize();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    left_child_->print(os, tab + 1);
    right_child_->print(os, tab + 1, left_child_->estCardinality());
}

double NLJoin::estCost(const double) const
{
    double lcard = left_child_->estCardinality();
    return left_child_->estCost() + lcard * right_child_->estCost(lcard);
}

}  // namespace cardinality
