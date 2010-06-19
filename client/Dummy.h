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

#ifndef CARDINALITY_DUMMY_H_
#define CARDINALITY_DUMMY_H_

#include "client/Operator.h"


namespace cardinality {

class Dummy: public Operator {
public:
    explicit Dummy(const NodeID);
    ~Dummy();
    Operator::Ptr clone() const;

    void Open(const Chunk * = NULL);
    void ReOpen(const Chunk * = NULL);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;

#ifdef PRINT_PLAN
    void print(std::ostream &, const int, const double) const;
#endif
    bool hasCol(const ColName) const;
    ColID getInputColID(const ColName) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const ColName) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const ColName) const;

    double estCost(const double = 0.0) const;
    double estCardinality(const bool = false) const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

private:
    Dummy& operator=(const Dummy &);
};

}  // namespace cardinality

#endif  // CARDINALITY_DUMMY_H_
