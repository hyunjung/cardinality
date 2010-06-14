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

#ifndef CARDINALITY_OPERATOR_H_
#define CARDINALITY_OPERATOR_H_

#include <vector>
#include <utility>  // std::pair
#include <iostream>  // std::ostream
#include <boost/smart_ptr/make_shared.hpp>
#include <google/protobuf/io/coded_stream.h>
#include "include/client.h"


namespace cardinality {

typedef uint16_t ColID;
typedef uint32_t NodeID;

// pointer and length for each value
typedef std::vector<std::pair<const char *, uint32_t> > Tuple;

class PartStats;

class Operator {
public:
    typedef boost::shared_ptr<Operator> Ptr;

    explicit Operator(const NodeID);
    explicit Operator(google::protobuf::io::CodedInputStream *);
    Operator(const Operator &);
    virtual ~Operator();
    virtual Operator::Ptr clone() const = 0;

    virtual void Open(const char * = NULL, const uint32_t = 0) = 0;
    virtual void ReOpen(const char * = NULL, const uint32_t = 0) = 0;
    virtual bool GetNext(Tuple &) = 0;
    virtual void Close() = 0;

    virtual uint8_t *SerializeToArray(uint8_t *) const;
    virtual int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    virtual void print(std::ostream &, const int = 0, const double = 0.0) const = 0;
#endif
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual std::pair<const PartStats *, ColID> getPartStats(const ColID) const = 0;
    virtual ValueType getColType(const char *) const = 0;
    virtual ColID numOutputCols() const = 0;
    virtual ColID getOutputColID(const char *) const = 0;

    virtual double estCost(const double = 0.0) const = 0;
    virtual double estCardinality(const bool = false) const = 0;
    virtual double estTupleLength() const = 0;
    virtual double estColLength(const ColID) const = 0;

    NodeID node_id() const;

    static uint32_t parseInt(const char *, const uint32_t);
    static Operator::Ptr parsePlan(google::protobuf::io::CodedInputStream *);

protected:
    enum { TAG_SEQSCAN, TAG_INDEXSCAN, TAG_NLJOIN, TAG_NBJOIN, TAG_REMOTE, TAG_UNION };

    NodeID node_id_;

private:
    Operator& operator=(const Operator &);
};

}  // namespace cardinality

#endif  // CARDINALITY_OPERATOR_H_
