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

#include "client/Dummy.h"
#include <stdexcept>  // std::runtime_error


namespace cardinality {

Dummy::Dummy(const NodeID n)
    : Operator(n)
{
}

Dummy::~Dummy()
{
}

Operator::Ptr Dummy::clone() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

void Dummy::Open(const Chunk *)
{
}

void Dummy::ReOpen(const Chunk *)
{
}

bool Dummy::GetNext(Tuple &tuple)
{
    return true;
}

void Dummy::Close()
{
}

uint8_t *Dummy::SerializeToArray(uint8_t *target) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

int Dummy::ByteSize() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

#ifdef PRINT_PLAN
void Dummy::print(std::ostream &os, const int tab, const double) const
{
    os << std::string(4 * tab, ' ');
    os << "Dummy@" << node_id();
    os << std::endl;
}
#endif

bool Dummy::hasCol(const ColName col) const
{
    return false;
}

ColID Dummy::getInputColID(const ColName col) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

std::pair<const PartStats *, ColID> Dummy::getPartStats(const ColID) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

ValueType Dummy::getColType(const ColName col) const
{
    return INT;
}

ColID Dummy::numOutputCols() const
{
    return 0;
}

ColID Dummy::getOutputColID(const ColName col) const
{
    return 0;
}

double Dummy::estCost(const double) const
{
    return 0.0;
}

double Dummy::estCardinality(const bool) const
{
    return 0.0;
}

double Dummy::estTupleLength() const
{
    return 0.0;
}

double Dummy::estColLength(const ColID cid) const
{
    return 0.0;
}

}  // namespace cardinality
