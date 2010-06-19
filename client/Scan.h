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

#ifndef CARDINALITY_SCAN_H_
#define CARDINALITY_SCAN_H_

#include <string>
#include <vector>
#include <boost/tuple/tuple.hpp>
#ifdef DISABLE_MEMORY_MAPPED_IO
#include <fstream>
#include <boost/smart_ptr/scoped_array.hpp>
#else
#include <utility>  // std::pair
#endif
#include "client/Project.h"
#include "client/PartStats.h"


namespace cardinality {

enum CompOp {
    EQ,
    GT
};

class Scan: public Project {
public:
    Scan(const NodeID, const char *, const char *,
         const Table *, const PartStats *, const Query *);
    explicit Scan(google::protobuf::io::CodedInputStream *);
    Scan(const Scan &);
    ~Scan();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    bool hasCol(const ColName) const;
    ColID getInputColID(const ColName) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const ColName) const;

    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    void initFilter(const Query *q);
    bool execFilter(const Tuple &) const;
    void execProject(const Tuple &, Tuple &) const;
    const char *parseLine(const char *);

    std::string filename_;
    std::vector<boost::tuple<Value *, ColID, CompOp> > gteq_conds_;
    std::vector<boost::tuple<ColID, ColID, bool> > join_conds_;
    uint32_t num_input_cols_;

    const std::string alias_;
    const Table *table_;
    const PartStats *stats_;
#ifdef DISABLE_MEMORY_MAPPED_IO
    std::ifstream file_;
    boost::scoped_array<char> buffer_;
#else
    std::pair<const char *, const char *> file_;
#endif
    Tuple input_tuple_;

    static const double COST_DISK_READ_PAGE = 1.0;
    static const double COST_DISK_SEEK_PAGE = 0.3;
    static const double SELECTIVITY_EQ = 0.05;
    static const double SELECTIVITY_GT = 0.4;

private:
    Scan& operator=(const Scan &);
};

}  // namespace cardinality

#endif  // CARDINALITY_SCAN_H_
