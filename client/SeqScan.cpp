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

    target = CodedOutputStream::WriteTagToArray(1, target);

    target = Scan::SerializeToArray(target);

    return target;
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
