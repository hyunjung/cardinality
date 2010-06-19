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

#include "client/Union.h"
#include <string>
#include <cstring>
#include <algorithm>  // std::min
#include "client/PartStats.h"
#include "client/util.h"


namespace cardinality {

static bool lessPivot(const std::pair<const Value *, uint32_t> &a,
                      const std::pair<const Value *, uint32_t> &b)
{
    return compareValue(a.first, b.first) < 0;
}

Union::Union(const NodeID n, std::vector<Operator::Ptr> c, const char *col)
    : Operator(n),
      children_(c),
      pivots_(),
      it_(),
      deserialized_(false),
      done_()
{
    if (col) {
        for (std::size_t i = 0; i < children_.size(); ++i) {
            std::pair<const PartStats *, ColID> x
                = children_[i]->getPartStats(getOutputColID(col));
            if (x.second != 0) {
                break;
            }
            // IndexScans with an equality condition on primary key
            pivots_.push_back(std::make_pair(&x.first->min_pkey_, i));
        }

        std::sort(pivots_.begin(), pivots_.end(), lessPivot);
    }
}

Union::Union(google::protobuf::io::CodedInputStream *input)
    : Operator(input),
      children_(),
      pivots_(),
      it_(),
      deserialized_(true),
      done_()
{
    Deserialize(input);
}

Union::Union(const Union &x)
    : Operator(x),
      children_(),
      pivots_(x.pivots_),
      it_(),
      deserialized_(false),
      done_()
{
    children_.reserve(x.children_.size());
    for (std::vector<Operator::Ptr>::const_iterator it = x.children_.begin();
         it < x.children_.end(); ++it) {
        children_.push_back((*it)->clone());
    }
}

Union::~Union()
{
    if (deserialized_) {
        for (std::size_t i = 0; i < pivots_.size(); ++i) {
            delete pivots_[i].first;
        }
    }
}

Operator::Ptr Union::clone() const
{
    return boost::make_shared<Union>(*this);
}

void Union::Open(const char *left_ptr, const uint32_t left_len)
{
    if (pivots_.empty()) {
        done_.resize(children_.size());
        for (std::size_t i = 0; i < children_.size(); ++i) {
            children_[i]->Open(left_ptr, left_len);
            done_[i] = false;
        }
        it_ = 0;

    } else {
        Value val;
        if (pivots_[0].first->type == INT) {
            val.type = INT;
            val.intVal = Operator::parseInt(left_ptr, left_len);
        } else {  // STRING
            val.type = STRING;
            val.intVal = left_len;
            std::memcpy(val.charVal, left_ptr, left_len);
            val.charVal[left_len] = '\0';
        }

        std::pair<const Value *, uint32_t> x = std::make_pair(&val, 0);
        std::vector<std::pair<const Value *, uint32_t> >::const_iterator it
            = std::upper_bound(pivots_.begin(), pivots_.end(), x, lessPivot);
        it_ = (it != pivots_.begin()) ? (it - 1)->second : 1;

        children_[it_]->Open(left_ptr, left_len);
    }
}

void Union::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    if (pivots_.empty()) {
        for (std::size_t i = 0; i < children_.size(); ++i) {
            children_[i]->ReOpen(left_ptr, left_len);
            done_[i] = false;
        }
        it_ = 0;

    } else {
        Close();
        Open(left_ptr, left_len);
    }
}

bool Union::GetNext(Tuple &tuple)
{
    if (!pivots_.empty()) {
        return children_[it_]->GetNext(tuple);
    }

    for (; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return false;
            }
        }
    }

    for (it_ = 0; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return false;
            }
        }
    }

    return true;
}

void Union::Close()
{
    if (pivots_.empty()) {
        for (std::size_t i = 0; i < children_.size(); ++i) {
            children_[i]->Close();
        }

    } else {
        children_[it_]->Close();
    }
}

uint8_t *Union::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(TAG_UNION, target);

    target = Operator::SerializeToArray(target);

    target = CodedOutputStream::WriteVarint32ToArray(children_.size(), target);
    for (std::size_t i = 0; i < children_.size(); ++i) {
        target = children_[i]->SerializeToArray(target);
    }

    target = CodedOutputStream::WriteVarint32ToArray(pivots_.size(), target);
    for (std::size_t i = 0; i < pivots_.size(); ++i) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     pivots_[i].first->type, target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     pivots_[i].first->intVal, target);
        if (pivots_[i].first->type == STRING) {
            int len = std::strlen(pivots_[i].first->charVal);
            target = CodedOutputStream::WriteVarint32ToArray(len, target);
            target = CodedOutputStream::WriteRawToArray(
                         pivots_[i].first->charVal, len, target);
        }

        target = CodedOutputStream::WriteVarint32ToArray(
                     pivots_[i].second, target);
    }

    return target;
}

int Union::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = 1 + Operator::ByteSize();

    total_size += CodedOutputStream::VarintSize32(children_.size());
    for (std::size_t i = 0; i < children_.size(); ++i) {
        total_size += children_[i]->ByteSize();
    }

    total_size += CodedOutputStream::VarintSize32(pivots_.size());
    for (std::size_t i = 0; i < pivots_.size(); ++i) {
        total_size += 1;
        total_size += CodedOutputStream::VarintSize32(pivots_[i].first->intVal);
        if (pivots_[i].first->type == STRING) {
            int len = std::strlen(pivots_[i].first->charVal);
            total_size += CodedOutputStream::VarintSize32(len);
            total_size += len;
        }

        total_size += CodedOutputStream::VarintSize32(pivots_[i].second);
    }

    return total_size;
}

void Union::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    uint32_t size;
    input->ReadVarint32(&size);
    children_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        children_.push_back(parsePlan(input));
    }

    input->ReadVarint32(&size);
    pivots_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        Value *value = new Value();
        uint32_t temp;
        input->ReadVarint32(&temp);
        value->type = static_cast<ValueType>(temp);
        input->ReadVarint32(&value->intVal);
        if (value->type == STRING) {
            input->ReadVarint32(&temp);
            input->ReadRaw(value->charVal, temp);
            value->charVal[temp] = '\0';
        }
        input->ReadVarint32(&temp);
        pivots_.push_back(std::make_pair(value, temp));
    }
}

#ifdef PRINT_PLAN
void Union::print(std::ostream &os, const int tab, const double lcard) const
{
    os << std::string(4 * tab, ' ');
    os << "Union@" << node_id();
    os << " card=" << estCardinality();
    os << " cost=" << estCost(lcard);
    os << std::endl;

    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->print(os, tab + 1, lcard);
    }
}
#endif

bool Union::hasCol(const ColName col) const
{
    return children_[0]->hasCol(col);
}

ColID Union::getInputColID(const ColName col) const
{
    return children_[0]->getOutputColID(col);
}

std::pair<const PartStats *, ColID> Union::getPartStats(const ColID cid) const
{
    return children_[0]->getPartStats(cid);
}

ValueType Union::getColType(const ColName col) const
{
    return children_[0]->getColType(col);
}

ColID Union::numOutputCols() const
{
    return children_[0]->numOutputCols();
}

ColID Union::getOutputColID(const ColName col) const
{
    return children_[0]->getOutputColID(col);
}

double Union::estCost(const double lcard) const
{
    double cost = 0.0;
    for (std::size_t i = 0; i < children_.size(); ++i) {
        cost += children_[i]->estCost(lcard);
    }

    if (!pivots_.empty()) {
        cost /= children_.size();
    }
    return cost;
}

double Union::estCardinality(const bool) const
{
    double card = 0.0;
    for (std::size_t i = 0; i < children_.size(); ++i) {
        card += children_[i]->estCardinality();
    }

    if (!pivots_.empty()) {
        card /= children_.size();
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

}  // namespace cardinality
