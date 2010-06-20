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

// typedef's
typedef uint16_t ColID;
typedef const char * ColName;  // alias.column
typedef uint32_t NodeID;

// typedef's for passing data between operators
typedef std::pair<const char *, uint32_t> Chunk;
typedef std::vector<Chunk> Tuple;

// defined in PartStats.cpp
class PartStats;

// Abstract base class for physical operators.
class Operator {
public:
    // Represents a query plan
    typedef boost::shared_ptr<Operator> Ptr;

    // Constructor, Destructor ---------------------------------------

    // Constructor called by the query planner in client.cpp.
    explicit Operator(const NodeID);

    // Deserialization constructor called by Operator::parsePlan().
    explicit Operator(google::protobuf::io::CodedInputStream *);

    // Copy constructor called by clone().
    Operator(const Operator &);

    // Virtual destructor.
    virtual ~Operator();

    // Deep copy.
    virtual Operator::Ptr clone() const = 0;

    // Query Execution -----------------------------------------------

    // Open this plan for execution.
    // The parameter indicates a join value for nested-loop index join.
    // The caller should ensure that this is closed.
    virtual void Open(const Chunk * = NULL) = 0;

    // Equivalent to (but more efficient than) Close() followed by Open().
    // The caller should ensure that this is open.
    virtual void ReOpen(const Chunk * = NULL) = 0;

    // Get the next tuple.
    // Returns true if there is no more result tuple.
    // The caller should ensure that no more call to GetNext() is made
    // once GetNext() returns true.
    virtual bool GetNext(Tuple &) = 0;

    // Close this plan.
    // The caller should ensure that this is open.
    virtual void Close() = 0;

    // Serialization -------------------------------------------------

    // Serialize this plan to the provided buffer.
    // The buffer should be at least ByteSize() bytes.
    // Member variables for execution states are not saved in the buffer.
    virtual uint8_t *SerializeToArray(uint8_t *) const;

    // Compute the serialized size of this plan.
    virtual int ByteSize() const;

    // Deserialize the given stream into this object.
    // Called by the deserialization constructor.
    void Deserialize(google::protobuf::io::CodedInputStream *);

    // Plan Exploration ----------------------------------------------

    // Print this plan for debugging.
    virtual void
        print(std::ostream &, const int = 0, const double = 0.0) const = 0;

    // Returns true if this plan scans the table containing the given
    // column.
    virtual bool hasCol(const ColName) const = 0;

    // Returns the column id corresponding to the given column
    // in an input tuple (a tuple passed from a child operator).
    // If there are 2 input tuples (e.g. Join) and the given column
    // appears in the inner, getInputColID() adds the number of
    // columns in the outer.
    // Throws std::runtime_error if the column is not found.
    virtual ColID getInputColID(const ColName) const = 0;

    // Returns the partition stats and the original column id
    // corresponding to the given output column id.
    virtual std::pair<const PartStats *, ColID>
        getPartStats(const ColID) const = 0;

    // Returns the type of the given column.
    // Throws std::runtime_error if the column is not found.
    virtual ValueType getColType(const ColName) const = 0;

    // Returns the number of columns in an output tuple.
    virtual ColID numOutputCols() const = 0;

    // Returns the column id corresponding to the given column
    // in an output tuple (a tuple passed to the parent operator).
    // Throws std::runtime_error if the column is not found.
    virtual ColID getOutputColID(const ColName) const = 0;

    // Cost Estimation -----------------------------------------------

    // Estimate cost for executing this plan.
    // The parameter indicates estimated cardinality of the outer plan
    // for nested-loop index join.
    virtual double estCost(const double = 0.0) const = 0;

    // Estimate the number of result tuples for this plan.
    // If the parameter is true, estCardinality() estimates the number
    // of matching tuples per a join value for nested-loop index join.
    virtual double estCardinality(const bool = false) const = 0;

    // Estimate the size of result tuple.
    virtual double estTupleSize() const = 0;

    // Estimate the size of a particular column.
    virtual double estColSize(const ColID) const = 0;

    // Others --------------------------------------------------------

    // Accessor
    NodeID node_id() const;

    // Parse an integer from a Chunk.
    static uint32_t parseInt(const Chunk *);

    // Construct a plan from the given serialized stream produced by
    // Remote::Open() using deserialization constructors.
    static Operator::Ptr parsePlan(google::protobuf::io::CodedInputStream *);

protected:
    // Tags indicating operator types in a serialized plan.
    enum { TAG_SEQSCAN, TAG_INDEXSCAN, TAG_NLJOIN, TAG_NBJOIN,
	   TAG_REMOTE, TAG_UNION };

    // operator description
    NodeID node_id_;

private:
    Operator& operator=(const Operator &);
};

}  // namespace cardinality

#endif  // CARDINALITY_OPERATOR_H_
