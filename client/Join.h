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

#ifndef CLIENT_JOIN_H_
#define CLIENT_JOIN_H_

#include <vector>
#include <boost/tuple/tuple.hpp>
#include "client/Project.h"


namespace cardinality {

class Join: public Project {
public:
    Join(const NodeID, Operator::Ptr, Operator::Ptr,
         const Query *, const int = -1);
    explicit Join(google::protobuf::io::CodedInputStream *);
    Join(const Join &);
    ~Join();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const char *) const;

    double estCardinality(const bool = false) const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    void initFilter(const Query *q, const int);
    bool execFilter(const Tuple &, const Tuple &) const;
    void execProject(const Tuple &, const Tuple &, Tuple &) const;

    Operator::Ptr left_child_;
    Operator::Ptr right_child_;
    std::vector<boost::tuple<ColID, ColID, bool, bool> > join_conds_;

    Tuple left_tuple_;
    Tuple right_tuple_;

private:
    Join& operator=(const Join &);
};

}  // namespace cardinality

#endif  // CLIENT_JOIN_H_
