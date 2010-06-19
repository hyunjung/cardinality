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

#ifndef CARDINALITY_NBJOIN_H_
#define CARDINALITY_NBJOIN_H_

#include <tr1/unordered_map>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include <boost/smart_ptr/scoped_array.hpp>
#include "client/Join.h"


namespace cardinality {

class NBJoin: public Join {
public:
    NBJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *);
    explicit NBJoin(google::protobuf::io::CodedInputStream *);
    NBJoin(const NBJoin &);
    ~NBJoin();
    Operator::Ptr clone() const;

    void Open(const Chunk * = NULL);
    void ReOpen(const Chunk * = NULL);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    void print(std::ostream &, const int, const double) const;
#endif

    double estCost(const double = 0.0) const;

protected:
    static uint64_t hashString(const char *, const uint32_t);

    enum { STATE_OPEN, STATE_REOPEN, STATE_GETNEXT, STATE_SWEEPBUFFER } state_;
    bool left_done_;
    typedef std::tr1::unordered_multimap<uint64_t, Tuple> multimap;
    boost::scoped_ptr<multimap> left_tuples_;
    multimap::const_iterator left_tuples_it_;
    multimap::const_iterator left_tuples_end_;
    boost::scoped_array<char> main_buffer_;
    boost::scoped_array<char> overflow_buffer_;

private:
    NBJoin& operator=(const NBJoin &);
};

}  // namespace cardinality

#endif  // CARDINALITY_NBJOIN_H_
