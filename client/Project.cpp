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

#include "client/Project.h"
#include <algorithm>
#include <set>


namespace cardinality {

Project::Project(const NodeID n)
    : Operator(n),
      selected_input_col_ids_()
{
}

Project::Project(google::protobuf::io::CodedInputStream *input)
    : Operator(input),
      selected_input_col_ids_()
{
    Deserialize(input);
}

Project::Project(const Project &x)
    : Operator(x),
      selected_input_col_ids_(x.selected_input_col_ids_)
{
}

Project::~Project()
{
}

uint8_t *Project::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = Operator::SerializeToArray(target);

    target = CodedOutputStream::WriteVarint32ToArray(
                 selected_input_col_ids_.size(), target);
    for (std::size_t i = 0; i < selected_input_col_ids_.size(); ++i) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     selected_input_col_ids_[i], target);
    }

    return target;
}

int Project::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = Operator::ByteSize();

    total_size += CodedOutputStream::VarintSize32(
                      selected_input_col_ids_.size());
    for (std::size_t i = 0; i < selected_input_col_ids_.size(); ++i) {
        total_size += CodedOutputStream::VarintSize32(
                          selected_input_col_ids_[i]);
    }

    return total_size;
}

void Project::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    uint32_t size;
    input->ReadVarint32(&size);
    selected_input_col_ids_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        uint32_t col_id;
        input->ReadVarint32(&col_id);
        selected_input_col_ids_.push_back(col_id);
    }
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
            && selected_cols.find(std::string(q->outputFields[i]))
               == selected_cols.end()) {
            selected_cols.insert(std::string(q->outputFields[i]));
            selected_input_col_ids_.push_back(
                getInputColID(q->outputFields[i]));
        }
    }
    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && !hasCol(q->joinFields2[i])
            && selected_cols.find(std::string(q->joinFields1[i]))
               == selected_cols.end()) {
            selected_cols.insert(std::string(q->joinFields1[i]));
            selected_input_col_ids_.push_back(
                getInputColID(q->joinFields1[i]));
        } else if (hasCol(q->joinFields2[i]) && !hasCol(q->joinFields1[i])
                   && selected_cols.find(std::string(q->joinFields2[i]))
                      == selected_cols.end()) {
            selected_cols.insert(std::string(q->joinFields2[i]));
            selected_input_col_ids_.push_back(
                getInputColID(q->joinFields2[i]));
        }
    }
}

}  // namespace cardinality
