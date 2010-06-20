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

#include "client/Join.h"
#include <cstring>
#include <algorithm>  // std::max
#include "client/PartStats.h"


namespace cardinality {

Join::Join(const NodeID n, Operator::Ptr l, Operator::Ptr r,
           const Query *q, const int x)
    : Project(n),
      left_child_(l),
      right_child_(r),
      join_conds_(),
      left_tuple_(), right_tuple_()
{
    initProject(q);
    initFilter(q, x);
}

Join::Join(google::protobuf::io::CodedInputStream *input)
    : Project(input),
      left_child_(),
      right_child_(),
      join_conds_(),
      left_tuple_(), right_tuple_()
{
    Deserialize(input);
}

Join::Join(const Join &x)
    : Project(x),
      left_child_(x.left_child_->clone()),
      right_child_(x.right_child_->clone()),
      join_conds_(x.join_conds_),
      left_tuple_(), right_tuple_()
{
}

Join::~Join()
{
}

uint8_t *Join::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = Project::SerializeToArray(target);

    target = CodedOutputStream::WriteVarint32ToArray(
                 join_conds_.size(), target);
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<0>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<1>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<2>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<3>(), target);
    }

    target = left_child_->SerializeToArray(target);
    target = right_child_->SerializeToArray(target);

    return target;
}

int Join::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = Project::ByteSize();

    total_size += CodedOutputStream::VarintSize32(join_conds_.size());
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        total_size += CodedOutputStream::VarintSize32(join_conds_[i].get<0>());
        total_size += CodedOutputStream::VarintSize32(join_conds_[i].get<1>());
        total_size += 2;
    }

    total_size += left_child_->ByteSize();
    total_size += right_child_->ByteSize();

    return total_size;
}

void Join::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    uint32_t size;
    input->ReadVarint32(&size);
    join_conds_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        uint32_t col_id_1;
        uint32_t col_id_2;
        uint32_t type;
        uint32_t skip;
        input->ReadVarint32(&col_id_1);
        input->ReadVarint32(&col_id_2);
        input->ReadVarint32(&type);
        input->ReadVarint32(&skip);

        join_conds_.push_back(
            boost::make_tuple(static_cast<ColID>(col_id_1),
                              static_cast<ColID>(col_id_2),
                              static_cast<bool>(type),
                              static_cast<bool>(skip)));
    }

    left_child_ = parsePlan(input);
    right_child_ = parsePlan(input);
}

void Join::initFilter(const Query *q, const int x)
{
    for (int i = 0; i < q->nbJoins; ++i) {
        if (right_child_->hasCol(q->joinFields1[i])
            && left_child_->hasCol(q->joinFields2[i])) {
            join_conds_.push_back(
                boost::make_tuple(
                    left_child_->getOutputColID(q->joinFields2[i]),
                    right_child_->getOutputColID(q->joinFields1[i]),
                    right_child_->getColType(q->joinFields1[i]),
                    i == x));
        } else if (right_child_->hasCol(q->joinFields2[i])
                   && left_child_->hasCol(q->joinFields1[i])) {
            join_conds_.push_back(
                boost::make_tuple(
                    left_child_->getOutputColID(q->joinFields1[i]),
                    right_child_->getOutputColID(q->joinFields2[i]),
                    right_child_->getColType(q->joinFields2[i]),
                    i == x));
        }
    }
}

bool Join::execFilter(const Tuple &left_tuple,
                      const Tuple &right_tuple) const
{
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        if (join_conds_[i].get<3>()) {  // already applied by IndexScan
            continue;
        }

        if (!join_conds_[i].get<2>()) {  // INT
            if (parseInt(&left_tuple[join_conds_[i].get<0>()])
                != parseInt(&right_tuple[join_conds_[i].get<1>()])) {
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

void Join::execProject(const Tuple &left_tuple,
                       const Tuple &right_tuple,
                       Tuple &output_tuple) const
{
    output_tuple.clear();

    for (ColID i = 0; i < numOutputCols(); ++i) {
        if (selected_input_col_ids_[i] < left_child_->numOutputCols()) {
            output_tuple.push_back(left_tuple[selected_input_col_ids_[i]]);
        } else {
            output_tuple.push_back(right_tuple[selected_input_col_ids_[i]
                                   - left_child_->numOutputCols()]);
        }
    }
}

bool Join::hasCol(const ColName col) const
{
    return right_child_->hasCol(col) || left_child_->hasCol(col);
}

ColID Join::getInputColID(const ColName col) const
{
    if (right_child_->hasCol(col)) {
        return right_child_->getOutputColID(col) + left_child_->numOutputCols();
    } else {
        return left_child_->getOutputColID(col);
    }
}

std::pair<const PartStats *, ColID> Join::getPartStats(const ColID cid) const
{
    if (selected_input_col_ids_[cid] > left_child_->numOutputCols()) {
        return right_child_->getPartStats(selected_input_col_ids_[cid]
                                          - left_child_->numOutputCols());
    } else {
        return left_child_->getPartStats(selected_input_col_ids_[cid]);
    }
}

ValueType Join::getColType(const ColName col) const
{
    if (right_child_->hasCol(col)) {
        return right_child_->getColType(col);
    } else {
        return left_child_->getColType(col);
    }
}

double Join::estCardinality(const bool) const
{
    double card = left_child_->estCardinality()
                  * right_child_->estCardinality();

    std::pair<const PartStats *, ColID> left_stats;
    std::pair<const PartStats *, ColID> right_stats;

    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        left_stats = left_child_->getPartStats(join_conds_[i].get<0>());
        right_stats = right_child_->getPartStats(join_conds_[i].get<1>());

        card /= std::max(left_stats.first
                             ->num_distinct_values_[left_stats.second],
                         right_stats.first
                             ->num_distinct_values_[right_stats.second]);
    }

    return card;
}

double Join::estTupleSize() const
{
    double length = 0.0;
    for (ColID i = 0; i < numOutputCols(); ++i) {
        length += estColSize(i);
    }

    return length;
}

double Join::estColSize(const ColID cid) const
{
    if (selected_input_col_ids_[cid] < left_child_->numOutputCols()) {
        return left_child_->estColSize(selected_input_col_ids_[cid]);
    } else {
        return right_child_->estColSize(selected_input_col_ids_[cid]
                                        - left_child_->numOutputCols());
    }
}

}  // namespace cardinality
