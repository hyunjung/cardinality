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

#include "client/Scan.h"
#include <cstring>
#include <stdexcept>  // std::runtime_error
#include <algorithm>  // std::min
#include <google/protobuf/wire_format_lite_inl.h>


namespace cardinality {

Scan::Scan(const NodeID n, const char *f, const char *a,
           const Table *t, const PartStats *p, const Query *q)
    : Project(n),
      filename_(f),
      gteq_conds_(), join_conds_(),
      num_input_cols_(t->nbFields),
      alias_(a), table_(t), stats_(p),
      file_(),
#ifdef DISABLE_MEMORY_MAPPED_IO
      buffer_(),
#endif
      input_tuple_()
{
    initProject(q);
    initFilter(q);
}

Scan::Scan(google::protobuf::io::CodedInputStream *input)
    : Project(input),
      filename_(),
      gteq_conds_(), join_conds_(),
      num_input_cols_(),
      alias_(), table_(), stats_(),
      file_(),
#ifdef DISABLE_MEMORY_MAPPED_IO
      buffer_(),
#endif
      input_tuple_()
{
    Deserialize(input);
}

Scan::Scan(const Scan &x)
    : Project(x),
      filename_(x.filename_),
      gteq_conds_(x.gteq_conds_), join_conds_(x.join_conds_),
      num_input_cols_(x.num_input_cols_),
      alias_(x.alias_), table_(x.table_), stats_(x.stats_),
      file_(),
#ifdef DISABLE_MEMORY_MAPPED_IO
      buffer_(),
#endif
      input_tuple_()
{
}

Scan::~Scan()
{
    // free objects allocated by Deserialize()
    if (alias_.empty()) {
        for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
            delete gteq_conds_[i].get<0>();
        }
    }
}

uint8_t *Scan::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = Project::SerializeToArray(target);

    target = CodedOutputStream::WriteVarint32ToArray(filename_.size(), target);
    target = CodedOutputStream::WriteStringToArray(filename_, target);

    target = CodedOutputStream::WriteVarint32ToArray(
                 gteq_conds_.size(), target);
    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     gteq_conds_[i].get<0>()->type, target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     gteq_conds_[i].get<0>()->intVal, target);
        if (gteq_conds_[i].get<0>()->type == STRING) {
            int len = std::strlen(gteq_conds_[i].get<0>()->charVal);
            target = CodedOutputStream::WriteVarint32ToArray(len, target);
            target = CodedOutputStream::WriteRawToArray(
                         gteq_conds_[i].get<0>()->charVal, len, target);
        }

        target = CodedOutputStream::WriteVarint32ToArray(
                     gteq_conds_[i].get<1>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     gteq_conds_[i].get<2>(), target);
    }

    target = CodedOutputStream::WriteVarint32ToArray(
                 join_conds_.size(), target);
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<0>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<1>(), target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     join_conds_[i].get<2>(), target);
    }

    target = CodedOutputStream::WriteVarint32ToArray(num_input_cols_, target);

    return target;
}

int Scan::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = Project::ByteSize();

    total_size += CodedOutputStream::VarintSize32(filename_.size());
    total_size += filename_.size();

    total_size += CodedOutputStream::VarintSize32(gteq_conds_.size());
    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        total_size += 1;
        total_size += CodedOutputStream::VarintSize32(
                          gteq_conds_[i].get<0>()->intVal);
        if (gteq_conds_[i].get<0>()->type == STRING) {
            int len = std::strlen(gteq_conds_[i].get<0>()->charVal);
            total_size += CodedOutputStream::VarintSize32(len);
            total_size += len;
        }

        total_size += CodedOutputStream::VarintSize32(gteq_conds_[i].get<1>());
        total_size += 1;
    }

    total_size += CodedOutputStream::VarintSize32(join_conds_.size());
    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        total_size += CodedOutputStream::VarintSize32(join_conds_[i].get<0>());
        total_size += CodedOutputStream::VarintSize32(join_conds_[i].get<1>());
        total_size += 1;
    }

    total_size += CodedOutputStream::VarintSize32(num_input_cols_);

    return total_size;
}

void Scan::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    google::protobuf::internal::WireFormatLite::ReadString(input, &filename_);

    uint32_t size;
    input->ReadVarint32(&size);
    gteq_conds_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        Value *value = new Value();
        uint32_t temp1, temp2;
        input->ReadVarint32(&temp1);
        value->type = static_cast<ValueType>(temp1);
        input->ReadVarint32(&value->intVal);
        if (value->type == STRING) {
            input->ReadVarint32(&temp2);
            input->ReadRaw(value->charVal, temp2);
            value->charVal[temp2] = '\0';
        }

        input->ReadVarint32(&temp1);
        input->ReadVarint32(&temp2);

        gteq_conds_.push_back(
            boost::make_tuple(value,
                              static_cast<ColID>(temp1),
                              static_cast<CompOp>(temp2)));
    }

    input->ReadVarint32(&size);
    join_conds_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        uint32_t col_id_1;
        uint32_t col_id_2;
        uint32_t type;
        input->ReadVarint32(&col_id_1);
        input->ReadVarint32(&col_id_2);
        input->ReadVarint32(&type);
        join_conds_.push_back(
            boost::make_tuple(static_cast<ColID>(col_id_1),
                              static_cast<ColID>(col_id_2),
                              static_cast<bool>(type)));
    }

    input->ReadVarint32(&num_input_cols_);
}

void Scan::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbRestrictionsEqual; ++i) {
        if (hasCol(q->restrictionEqualFields[i])) {
            gteq_conds_.push_back(
                boost::make_tuple(
                    &q->restrictionEqualValues[i],
                    getInputColID(q->restrictionEqualFields[i]),
                    EQ));
        }
    }

    for (int i = 0; i < q->nbRestrictionsGreaterThan; ++i) {
        if (hasCol(q->restrictionGreaterThanFields[i])) {
            gteq_conds_.push_back(
                boost::make_tuple(
                    &q->restrictionGreaterThanValues[i],
                    getInputColID(q->restrictionGreaterThanFields[i]),
                    GT));
        }
    }

    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && hasCol(q->joinFields2[i])) {
            join_conds_.push_back(
                boost::make_tuple(
                    getInputColID(q->joinFields1[i]),
                    getInputColID(q->joinFields2[i]),
                    table_->fieldsType[getInputColID(q->joinFields1[i])]));
        }
    }
}

const char * Scan::parseLine(const char *pos)
{
    input_tuple_.clear();

    for (std::size_t i = 0; i < num_input_cols_ - 1; ++i) {
        const char *delim = static_cast<const char *>(rawmemchr(pos, '|'));
        input_tuple_.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

#ifdef DISABLE_MEMORY_MAPPED_IO
    const char *delim = static_cast<const char *>(rawmemchr(pos, '\0'));
#else
    const char *delim = static_cast<const char *>(rawmemchr(pos, '\n'));
#endif
    input_tuple_.push_back(std::make_pair(pos, delim - pos));
    return delim + 1;
}

bool Scan::execFilter(const Tuple &tuple) const
{
    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        if (gteq_conds_[i].get<0>()->type == INT) {
            int cmp = gteq_conds_[i].get<0>()->intVal
                      - parseInt(&tuple[gteq_conds_[i].get<1>()]);
            if ((gteq_conds_[i].get<2>() == EQ && cmp != 0)
                || (gteq_conds_[i].get<2>() == GT && cmp >= 0)) {
                return false;
            }
        } else {  // STRING
            if (gteq_conds_[i].get<2>() == EQ) {
                if ((gteq_conds_[i].get<0>()->intVal
                     != tuple[gteq_conds_[i].get<1>()].second)
                    || (std::memcmp(gteq_conds_[i].get<0>()->charVal,
                                    tuple[gteq_conds_[i].get<1>()].first,
                                    tuple[gteq_conds_[i].get<1>()].second))) {
                    return false;
                }
            } else {  // GT
                int cmp = std::memcmp(
                              gteq_conds_[i].get<0>()->charVal,
                              tuple[gteq_conds_[i].get<1>()].first,
                              std::min(gteq_conds_[i].get<0>()->intVal,
                                       tuple[gteq_conds_[i].get<1>()].second));
                if (cmp > 0
                    || (cmp == 0
                        && gteq_conds_[i].get<0>()->intVal
                           >= tuple[gteq_conds_[i].get<1>()].second)) {
                    return false;
                }
            }
        }
    }

    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        if (!join_conds_[i].get<2>()) {  // INT
            if (parseInt(&tuple[join_conds_[i].get<0>()])
                != parseInt(&tuple[join_conds_[i].get<1>()])) {
                return false;
            }
        } else {  // STRING
            if ((tuple[join_conds_[i].get<0>()].second
                 != tuple[join_conds_[i].get<1>()].second)
                || (std::memcmp(tuple[join_conds_[i].get<0>()].first,
                                tuple[join_conds_[i].get<1>()].first,
                                tuple[join_conds_[i].get<1>()].second))) {
                return false;
            }
        }
    }

    return true;
}

void Scan::execProject(const Tuple &input_tuple,
                       Tuple &output_tuple) const
{
    output_tuple.clear();

    for (std::size_t i = 0; i < selected_input_col_ids_.size(); ++i) {
        output_tuple.push_back(input_tuple[selected_input_col_ids_[i]]);
    }
}

bool Scan::hasCol(const ColName col) const
{
    return (col[alias_.size()] == '.' || col[alias_.size()] == '\0')
           && !std::memcmp(col, alias_.data(), alias_.size());
}

ColID Scan::getInputColID(const ColName col) const
{
    const char *dot = std::strchr(col, '.');
    if (dot == NULL) {
        throw std::runtime_error("invalid column name");
    }

    for (int i = 0; i < table_->nbFields; ++i) {
        if (!std::strcmp(dot + 1, table_->fieldsName[i])) {
            return i;
        }
    }
    throw std::runtime_error("column name not found");
}

std::pair<const PartStats *, ColID> Scan::getPartStats(const ColID cid) const
{
    return std::make_pair(stats_, selected_input_col_ids_[cid]);
}

ValueType Scan::getColType(const ColName col) const
{
    return table_->fieldsType[getInputColID(col)];
}

double Scan::estTupleSize() const
{
    double length = 0;
    for (ColID i = 0; i < numOutputCols(); ++i) {
        length += estColSize(i);
    }

    return length;
}

double Scan::estColSize(const ColID cid) const
{
    return stats_->col_lengths_[selected_input_col_ids_[cid]];
}

}  // namespace cardinality
