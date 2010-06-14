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

#include "client/Operator.h"
#include <boost/spirit/include/qi.hpp>
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"


namespace cardinality {

Operator::Operator(const NodeID n)
    : node_id_(n)
{
}

Operator::Operator(google::protobuf::io::CodedInputStream *input)
    : node_id_()
{
    Deserialize(input);
}

Operator::Operator(const Operator &x)
    : node_id_(x.node_id_)
{
}

Operator::~Operator()
{
}

uint8_t *Operator::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteVarint32ToArray(node_id_, target);

    return target;
}

int Operator::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = CodedOutputStream::VarintSize32(node_id_);

    return total_size;
}

void Operator::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    input->ReadVarint32(&node_id_);
}

NodeID Operator::node_id() const
{
    return node_id_;
}

uint32_t Operator::parseInt(const char *str, const uint32_t len)
{
    uint32_t intval = 0;
    boost::spirit::qi::parse(str, str + len, boost::spirit::uint_, intval);
    return intval;
}

Operator::Ptr Operator::parsePlan(google::protobuf::io::CodedInputStream *input)
{
    uint32_t operator_type;
    input->ReadVarint32(&operator_type);

    Operator::Ptr plan;

    switch (operator_type) {
    case TAG_SEQSCAN:
        plan = boost::make_shared<SeqScan>(input);
        break;

    case TAG_INDEXSCAN:
        plan = boost::make_shared<IndexScan>(input);
        break;

    case TAG_NLJOIN:
        plan = boost::make_shared<NLJoin>(input);
        break;

    case TAG_NBJOIN:
        plan = boost::make_shared<NBJoin>(input);
        break;

    case TAG_REMOTE:
        plan = boost::make_shared<Remote>(input);
        break;

    case TAG_UNION:
        plan = boost::make_shared<Union>(input);
        break;
    }

    return plan;
}

}  // namespace cardinality
