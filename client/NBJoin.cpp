#include "client/NBJoin.h"
#include <cstring>


namespace cardinality {

static const int NBJOIN_BUFSIZE = 65536;

NBJoin::NBJoin(const NodeID n, Operator::Ptr l, Operator::Ptr r,
               const Query *q)
    : Join(n, l, r, q),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::NBJoin()
    : Join(),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::NBJoin(const NBJoin &x)
    : Join(x),
      state_(), left_done_(),
      left_tuples_(),
      left_tuples_it_(),
      left_tuples_end_(),
      right_tuple_(),
      main_buffer_(), overflow_buffer_()
{
}

NBJoin::~NBJoin()
{
}

Operator::Ptr NBJoin::clone() const
{
    return boost::make_shared<NBJoin>(*this);
}

void NBJoin::Open(const char *, const uint32_t)
{
    main_buffer_.reset(new char[NBJOIN_BUFSIZE]);
    left_tuples_.reset(new multimap());
    state_ = RIGHT_OPEN;
    right_tuple_.reserve(right_child_->numOutputCols());
    left_child_->Open();
}

void NBJoin::ReOpen(const char *, const uint32_t)
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

bool NBJoin::GetNext(Tuple &tuple)
{
    for (;;) {
        switch (state_) {
        case RIGHT_OPEN:
        case RIGHT_REOPEN: {
            Tuple left_tuple;
            left_tuple.reserve(left_child_->numOutputCols());
            for (char *pos = main_buffer_.get();
                 pos - main_buffer_.get() < NBJOIN_BUFSIZE - 512
                 && !(left_done_ = left_child_->GetNext(left_tuple)); ) {
                for (std::size_t i = 0; i < left_tuple.size(); ++i) {
                    uint32_t len = left_tuple[i].second;
                    if (pos + len < main_buffer_.get() + NBJOIN_BUFSIZE) {
                        std::memcpy(pos, left_tuple[i].first, len);
                        pos[len] = '\0';
                        left_tuple[i].first = pos;
                        pos += len + 1;
                    } else {  // main_buffer_ doesn't have enough space
                        int overflow_len = len + 1;
                        for (std::size_t j = i + 1;
                             j < left_tuple.size(); ++j) {
                            overflow_len += left_tuple[j].second + 1;
                        }
                        overflow_buffer_.reset(new char[overflow_len]);
                        pos = overflow_buffer_.get();
                        for (; i < left_tuple.size(); ++i) {
                            uint32_t len = left_tuple[i].second;
                            std::memcpy(pos, left_tuple[i].first, len);
                            pos[len] = '\0';
                            left_tuple[i].first = pos;
                            pos += len + 1;
                        }
                        pos = main_buffer_.get() + NBJOIN_BUFSIZE;
                    }
                }

                if (join_conds_.empty()) {  // cross product
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        0, left_tuple));
                } else if (join_conds_[0].get<2>()) {  // STRING
                    ColID cid = join_conds_[0].get<0>();
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        hashString(left_tuple[cid].first,
                                   left_tuple[cid].second),
                        left_tuple));
                } else {  // INT
                    ColID cid = join_conds_[0].get<0>();
                    left_tuples_->insert(std::pair<uint64_t, Tuple>(
                        parseInt(left_tuple[cid].first,
                                 left_tuple[cid].second),
                        left_tuple));
                }
            }

            if (left_tuples_->empty()) {
                if (state_ == RIGHT_REOPEN) {
                    right_child_->Close();
                }
                return true;
            }

            if (state_ == RIGHT_OPEN) {
                right_child_->Open();
            } else {  // RIGHT_REOPEN
                right_child_->ReOpen();
            }
            state_ = RIGHT_GETNEXT;
        }

        case RIGHT_GETNEXT:
            if (right_child_->GetNext(right_tuple_)) {
                left_tuples_->clear();
                if (left_done_) {
                    right_child_->Close();
                    return true;
                }
                state_ = RIGHT_REOPEN;
                break;
            }

            if (join_conds_.empty()) {  // cross product
                left_tuples_it_ = left_tuples_->begin();
                left_tuples_end_ = left_tuples_->end();
            } else if (join_conds_[0].get<2>()) {  // STRING
                ColID cid = join_conds_[0].get<1>();
                std::pair<multimap::const_iterator,
                          multimap::const_iterator> range
                    = left_tuples_->equal_range(
                          hashString(right_tuple_[cid].first,
                                     right_tuple_[cid].second));
                left_tuples_it_ = range.first;
                left_tuples_end_ = range.second;
            } else {  // INT
                ColID cid = join_conds_[0].get<1>();
                std::pair<multimap::const_iterator,
                          multimap::const_iterator> range
                    = left_tuples_->equal_range(
                          parseInt(right_tuple_[cid].first,
                                   right_tuple_[cid].second));
                left_tuples_it_ = range.first;
                left_tuples_end_ = range.second;
            }
            state_ = RIGHT_SWEEPBUFFER;

        case RIGHT_SWEEPBUFFER:
            for (; left_tuples_it_ != left_tuples_end_; ++left_tuples_it_) {
                if (execFilter(left_tuples_it_->second, right_tuple_)) {
                    execProject((left_tuples_it_++)->second,
                                right_tuple_, tuple);
                    return false;
                }
            }
            state_ = RIGHT_GETNEXT;
            break;
        }
    }

    return false;
}

void NBJoin::Close()
{
    left_tuples_.reset();
    main_buffer_.reset();
    overflow_buffer_.reset();
    left_child_->Close();
}

uint8_t *NBJoin::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(4, target);

    target = Join::SerializeToArray(target);

    return target;
}

int NBJoin::ByteSize() const
{
    int total_size = 1;

    total_size += Join::ByteSize();

    return total_size;
}

void NBJoin::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    Join::Deserialize(input);
}

void NBJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NBJoin@" << node_id();
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;

    left_child_->print(os, tab + 1);
    right_child_->print(os, tab + 1);
}

double NBJoin::estCost(const double) const
{
    return left_child_->estCost()
           + std::max(1.0, left_child_->estCardinality()
                           * left_child_->estTupleLength() / NBJOIN_BUFSIZE)
             * right_child_->estCost();
}

uint64_t NBJoin::hashString(const char *str, const uint32_t len)
{
    uint64_t hash = 0;
    std::memcpy(&hash, str, std::min(len, 8u));
    return hash;
}

}  // namespace cardinality
