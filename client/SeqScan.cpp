#include "client/SeqScan.h"


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
    return Operator::Ptr(new SeqScan(*this));
}

void SeqScan::Open(const char *, const uint32_t)
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    file_.open(filename_);
    pos_ = file_.begin();
#else
    line_buffer_.reset(new char[(MAX_VARCHAR_LEN + 1) * num_input_cols_]);
    file_.open(filename_.c_str());
    if (file_.fail()) {
        throw std::runtime_error("ifstream.open() failed");
    }
#endif
}

void SeqScan::ReOpen(const char *, const uint32_t)
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    pos_ = file_.begin();
#else
    file_.clear();
    file_.seekg(0, std::ios::beg);
#endif
}

bool SeqScan::GetNext(Tuple &tuple)
{
    Tuple temp;

#ifndef USE_STD_IFSTREAM_FOR_SCAN
    while (pos_ < file_.end()) {
        pos_ = splitLine(pos_, file_.end(), temp);

        if (execFilter(temp)) {
            execProject(temp, tuple);
            return false;
        }
    }

    return true;
#else
    for (;;) {
        if (file_.eof()) {
            return true;
        }
        file_.getline(line_buffer_.get(), (MAX_VARCHAR_LEN + 1) * num_input_cols_);
        if (*line_buffer_.get() == '\0') {
            return true;
        }

        temp.clear();

        const char *pos = line_buffer_.get();
        const char *eof = line_buffer_.get() + std::strlen(line_buffer_.get()) + 1;

        for (int i = 0; i < num_input_cols_; ++i) {
            const char *delim
                = static_cast<const char *>(
                      std::memchr(pos, (i == num_input_cols_ - 1) ? '\0' : '|', eof - pos));
            temp.push_back(std::make_pair(pos, delim - pos));
            pos = delim + 1;
        }

        if (execFilter(temp)) {
            execProject(temp, tuple);
            return false;
        }
    }
#endif
}

void SeqScan::Close()
{
    file_.close();
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

double SeqScan::estCost() const
{
    return stats_->num_pages_ * COST_DISK_READ_PAGE;
}

double SeqScan::estCardinality() const
{
    double card = stats_->cardinality_;

    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        if (gteq_conds_[i].get<2>() == EQ) {
            if (gteq_conds_[i].get<0>() == 0) {
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
