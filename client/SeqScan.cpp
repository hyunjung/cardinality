#include "client/SeqScan.h"
#include "client/Server.h"


extern cardinality::Server *g_server;  // client.cpp

namespace cardinality {

SeqScan::SeqScan(const NodeID n, const char *f, const char *a,
                 const Table *t, const PartitionStats *p, const Query *q)
    : Scan(n, f, a, t, p, q),
      pos_()
{
}

SeqScan::SeqScan()
    : Scan(),
      pos_()
{
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
    file_ = g_server->openFile(filename_);
    input_tuple_.reserve(num_input_cols_);
    pos_ = file_.first;
}

void SeqScan::ReOpen(const char *, const uint32_t)
{
    pos_ = file_.first;
}

bool SeqScan::GetNext(Tuple &tuple)
{
    while (pos_ < file_.second) {
        pos_ = parseLine(pos_);

        if (execFilter(input_tuple_)) {
            execProject(input_tuple_, tuple);
            return false;
        }
    }

    return true;
}

void SeqScan::Close()
{
//  file_.close();
}

void SeqScan::Serialize(google::protobuf::io::CodedOutputStream *output) const
{
    output->WriteVarint32(1);

    Scan::Serialize(output);
}

int SeqScan::ByteSize() const
{
    int total_size = 1;

    total_size += Scan::ByteSize();

    return total_size;
}

void SeqScan::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    Scan::Deserialize(input);
}

void SeqScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan@" << node_id() << " " << filename_;
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;
}

double SeqScan::estCost(const double) const
{
    return stats_->num_pages_ * COST_DISK_READ_PAGE;
}

double SeqScan::estCardinality() const
{
    double card = stats_->cardinality_;

    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        if (gteq_conds_[i].get<2>() == EQ) {
            if (gteq_conds_[i].get<1>() == 0) {
                card /= stats_->cardinality_;
            } else {
                card *= SELECTIVITY_EQ;
            }
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
