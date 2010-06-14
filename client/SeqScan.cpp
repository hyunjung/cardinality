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

#include "client/SeqScan.h"
#include "client/IOManager.h"


extern cardinality::IOManager *g_io_mgr;  // client.cpp

namespace cardinality {

SeqScan::SeqScan(const NodeID n, const char *f, const char *a,
                 const Table *t, const PartStats *p, const Query *q)
    : Scan(n, f, a, t, p, q),
      pos_()
{
}

SeqScan::SeqScan(google::protobuf::io::CodedInputStream *input)
    : Scan(input),
      pos_()
{
    Deserialize(input);
}

SeqScan::SeqScan(const SeqScan &x)
    : Scan(x),
      pos_()
{
}

SeqScan::~SeqScan()
{
}

Operator::Ptr SeqScan::clone() const
{
    return boost::make_shared<SeqScan>(*this);
}

void SeqScan::Open(const char *, const uint32_t)
{
#ifdef DISABLE_MEMORY_MAPPED_IO
    buffer_.reset(new char[4096]);
    file_.open(filename_.c_str(), std::ifstream::in | std::ifstream::binary);
#else
    file_ = g_io_mgr->openFile(filename_);
    pos_ = file_.first;
#endif
    input_tuple_.reserve(num_input_cols_);
}

void SeqScan::ReOpen(const char *, const uint32_t)
{
#ifdef DISABLE_MEMORY_MAPPED_IO
    file_.clear();
    file_.seekg(0, std::ios::beg);
#else
    pos_ = file_.first;
#endif
}

bool SeqScan::GetNext(Tuple &tuple)
{
#ifdef DISABLE_MEMORY_MAPPED_IO
    for (;;) {
        file_.getline(buffer_.get(), 4096);
        if (*buffer_.get() == '\0') {
            return true;
        }
        parseLine(buffer_.get());
#else
    while (pos_ < file_.second) {
        pos_ = parseLine(pos_);
#endif

        if (execFilter(input_tuple_)) {
            execProject(input_tuple_, tuple);
            return false;
        }
    }

    return true;
}

void SeqScan::Close()
{
#ifdef DISABLE_MEMORY_MAPPED_IO
    file_.close();
    buffer_.reset();
#endif
}

uint8_t *SeqScan::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(TAG_SEQSCAN, target);

    target = Scan::SerializeToArray(target);

    return target;
}

int SeqScan::ByteSize() const
{
    int total_size = 1 + Scan::ByteSize();

    return total_size;
}

void SeqScan::Deserialize(google::protobuf::io::CodedInputStream *input)
{
}

#ifdef PRINT_PLAN
void SeqScan::print(std::ostream &os, const int tab, const double) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan@" << node_id() << " " << filename_;
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;
}
#endif

double SeqScan::estCost(const double) const
{
    return stats_->num_pages_ * COST_DISK_READ_PAGE;
}

double SeqScan::estCardinality(const bool) const
{
    double card = stats_->num_distinct_values_[0];

    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        if (gteq_conds_[i].get<2>() == EQ) {
            card /= stats_->num_distinct_values_[gteq_conds_[i].get<1>()];
        } else {  // GT
            card *= SELECTIVITY_GT;
        }
    }

    if (!join_conds_.empty()) {
        card *= SELECTIVITY_EQ;
    }

    return card;
}

}  // namespace cardinality
