#include "client/IndexScan.h"
#include <cstring>
#include <stdexcept>  // std::runtime_error
#include <algorithm>  // std::sort, std::min
#include <google/protobuf/wire_format_lite_inl.h>
#include "client/IOManager.h"


extern cardinality::IOManager *g_io_mgr;  // client.cpp

namespace cardinality {

IndexScan::IndexScan(const NodeID n, const char *f, const char *a,
                     const Table *t, const PartStats *p, const Query *q,
                     const char *col)
    : Scan(n, f, a, t, p, q),
      index_col_(), index_col_type_(),
      comp_op_(), value_(NULL), index_col_id_(),
      index_(), addrs_(), i_()
{
    if (col) {  // NLIJ
        const char *dot = std::strchr(col, '.');
        if (dot == NULL) {
            throw std::runtime_error("invalid column name");
        }
        index_col_ = table_->tableName;
        index_col_ += '.';
        index_col_ += dot + 1;
        comp_op_ = EQ;
        index_col_type_ = getColType(col);
        index_col_id_ = getInputColID(col);
    } else {
        for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
            const char *col = table_->fieldsName[gteq_conds_[i].get<1>()];
            if (col[0] == '_') {
                index_col_ = table_->tableName;
                index_col_ += '.';
                index_col_ += col;
                comp_op_ = gteq_conds_[i].get<2>();
                value_ = gteq_conds_[i].get<0>();
                index_col_type_ = value_->type;
                index_col_id_ = gteq_conds_[i].get<1>();
                gteq_conds_.erase(gteq_conds_.begin() + i);
                break;
            }
        }

        if (index_col_.empty()) {
            throw std::runtime_error("index_ed column not found");
        }
    }
}

IndexScan::IndexScan(google::protobuf::io::CodedInputStream *input)
    : Scan(input),
      index_col_(), index_col_type_(),
      comp_op_(), value_(NULL), index_col_id_(),
      index_(), addrs_(), i_()
{
    Deserialize(input);
}

IndexScan::IndexScan(const IndexScan &x)
    : Scan(x),
      index_col_(x.index_col_), index_col_type_(x.index_col_type_),
      comp_op_(x.comp_op_), value_(x.value_), index_col_id_(x.index_col_id_),
      index_(), addrs_(), i_()
{
}

IndexScan::~IndexScan()
{
    // free objects allocated by Deserialize()
    if (alias_.empty()) {
        delete value_;
    }
}

Operator::Ptr IndexScan::clone() const
{
    return boost::make_shared<IndexScan>(*this);
}

void IndexScan::Open(const char *left_ptr, const uint32_t left_len)
{
#ifdef DISABLE_MEMORY_MAPPED_IO
    buffer_.reset(new char[4096]);
    file_.open(filename_.c_str(), std::ifstream::in | std::ifstream::binary);
#else
    file_ = g_io_mgr->openFile(filename_);
#endif
    input_tuple_.reserve(num_input_cols_);
    openIndex(index_col_.c_str(), &index_);

    ReOpen(left_ptr, left_len);
}

void IndexScan::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    TxnState *txn;
    Record record;
    bool check_index_cond = true;
    uint32_t key_intval;
    const char *key_charval = NULL;

    record.val.type = index_col_type_;
    if (index_col_type_ == INT) {
        if (left_ptr) {
            key_intval = parseInt(left_ptr, left_len);
        } else {
            key_intval = value_->intVal;
        }
        record.val.intVal = key_intval;
    } else {  // STRING
        if (left_ptr) {
            key_charval = left_ptr;
            key_intval = left_len;
        } else {
            key_charval = value_->charVal;
            key_intval = value_->intVal;
        }
        std::memcpy(record.val.charVal, key_charval, key_intval);
        record.val.charVal[key_intval] = '\0';
    }

    addrs_.clear();
    i_ = 0;

    beginTransaction(&txn);
    ErrCode ec = get(index_, txn, &record);

    // GT: the first tuple will be returned in the while loop
    switch (ec) {
    case SUCCESS:
        if (comp_op_ == EQ) {
            addrs_.push_back(record.address);

            if (index_col_id_ == 0) {
                goto commit;
            }
        }
        break;

    case KEY_NOTFOUND:
        if (comp_op_ == EQ) {
            goto commit;
        } else {  // GT
            check_index_cond = false;
        }
        break;

    default:
        throw std::runtime_error("get() failed");
    }

    while (getNext(index_, txn, &record) == SUCCESS) {
        if (check_index_cond) {
            if (index_col_type_ == INT) {
                if (comp_op_ == EQ) {
                    if (key_intval != record.val.intVal) {
                        break;
                    }
                } else {  // GT
                    if (key_intval >= record.val.intVal) {
                        continue;
                    }
                    check_index_cond = false;
                }
            } else {  // STRING
                if (comp_op_ == EQ) {
                    if (record.val.charVal[key_intval] != '\0'
                        || std::memcmp(key_charval, record.val.charVal,
                                       key_intval)) {
                        break;
                    }
                } else {  // GT
                    uint32_t charval_len = std::strlen(record.val.charVal);
                    int cmp = std::memcmp(key_charval, record.val.charVal,
                                          std::min(key_intval, charval_len));
                    if (cmp > 0 || (cmp == 0 && key_intval >= charval_len)) {
                        continue;
                    }
                    check_index_cond = false;
                }
            }
        }

        addrs_.push_back(record.address);
    }

    std::sort(addrs_.begin(), addrs_.end());

commit:
    commitTransaction(txn);
}

bool IndexScan::GetNext(Tuple &tuple)
{
    while (i_ < addrs_.size()) {
#ifdef DISABLE_MEMORY_MAPPED_IO
        file_.seekg(addrs_[i_++], std::ios::beg);
        file_.getline(buffer_.get(), 4096);
        parseLine(buffer_.get());
#else
        parseLine(file_.first + addrs_[i_++]);
#endif

        if (execFilter(input_tuple_)) {
            execProject(input_tuple_, tuple);
            return false;
        }
    }

    return true;
}

void IndexScan::Close()
{
    closeIndex(index_);
#ifdef DISABLE_MEMORY_MAPPED_IO
    file_.close();
    buffer_.reset();
#endif
}

uint8_t *IndexScan::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteTagToArray(2, target);

    target = Scan::SerializeToArray(target);

    target = CodedOutputStream::WriteVarint32ToArray(index_col_.size(), target);
    target = CodedOutputStream::WriteStringToArray(index_col_, target);

    target = CodedOutputStream::WriteVarint32ToArray(index_col_type_, target);
    target = CodedOutputStream::WriteVarint32ToArray(comp_op_, target);
    target = CodedOutputStream::WriteVarint32ToArray(index_col_id_, target);

    target = CodedOutputStream::WriteVarint32ToArray(!value_, target);
    if (value_) {
        target = CodedOutputStream::WriteVarint32ToArray(
                     value_->type, target);
        target = CodedOutputStream::WriteVarint32ToArray(
                     value_->intVal, target);
        if (value_->type == STRING) {
            int len = std::strlen(value_->charVal);
            target = CodedOutputStream::WriteVarint32ToArray(len, target);
            target = CodedOutputStream::WriteRawToArray(
                         value_->charVal, len, target);
        }
    }

    return target;
}

int IndexScan::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = 1 + Scan::ByteSize();

    total_size += CodedOutputStream::VarintSize32(index_col_.size());
    total_size += index_col_.size();

    total_size += 2;
    total_size += CodedOutputStream::VarintSize32(index_col_id_);

    total_size += 1;
    if (value_) {
        total_size += 1;
        total_size += CodedOutputStream::VarintSize32(value_->intVal);
        if (value_->type == STRING) {
            int len = std::strlen(value_->charVal);
            total_size += CodedOutputStream::VarintSize32(len);
            total_size += len;
        }
    }

    return total_size;
}

void IndexScan::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    google::protobuf::internal::WireFormatLite::ReadString(input, &index_col_);

    uint32_t temp;
    input->ReadVarint32(&temp);
    index_col_type_ = static_cast<ValueType>(temp);
    input->ReadVarint32(&temp);
    comp_op_ = static_cast<CompOp>(temp);
    input->ReadVarint32(&temp);
    index_col_id_ = static_cast<ColID>(temp);

    input->ReadVarint32(&temp);
    if (!temp) {
        value_ = new Value();
        input->ReadVarint32(&temp);
        value_->type = static_cast<ValueType>(temp);
        input->ReadVarint32(&value_->intVal);
        if (value_->type == STRING) {
            input->ReadVarint32(&temp);
            input->ReadRaw(value_->charVal, temp);
            value_->charVal[temp] = '\0';
        }
    }
}

#ifdef PRINT_PLAN
void IndexScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "IndexScan@" << node_id() << " " << filename_ << " ";
    if (index_col_id_ == 0) {
        os << "unique ";
    }
    os << index_col_ << ((comp_op_ == EQ) ? "=" : ">");
    if (!value_) {  // NLIJ
        os << "leftValue";
    } else if (value_->type == INT) {
        os << value_->intVal;
    } else {  // STRING
        os << "'" << value_->charVal << "'";
    }
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;
}
#endif

// Mackert and Lohman,
// Index Scans Using a Finite LRU Buffer: A Validated I/O Model,
// ACM Transactions on Database Systems, Vol. 14, No. 3, September 1989, p.411
//
static double MACKERT_LOHMAN(double T, double D, double x = 1.0)
{
    double Y;
    double b = 65536.0;  // 256 MB
    double Dx = D * x;

    if (T <= b) {
        Y = std::min(T, (2.0 * T * Dx) / (2.0 * T + Dx));
    } else {  // T > b
        double l = (2.0 * T * b) / (2.0 * T - b);
        if (Dx <= l) {
            Y = (2.0 * T * Dx) / (2.0 * T + Dx);
        } else {
            Y = b + (Dx - l) * (T - b) / T;
        }
    }

    return Y;
}

double IndexScan::estCost(const double left_cardinality) const
{
    double seq_pages = 0.0;
    double random_pages = 0.0;

    if (!value_) {  // NLIJ
        double num_dups = stats_->num_distinct_values_[0]
                          / stats_->num_distinct_values_[index_col_id_];
        random_pages = MACKERT_LOHMAN(stats_->num_pages_,
                                      num_dups, left_cardinality)
                       / left_cardinality;
    } else {
        if (comp_op_ == EQ) {
            if (index_col_id_ == 0) {
                seq_pages = MACKERT_LOHMAN(stats_->num_pages_, 1.0);
            } else {
                double num_dups = stats_->num_distinct_values_[0]
                                  / stats_->num_distinct_values_[index_col_id_];
                random_pages = MACKERT_LOHMAN(stats_->num_pages_, num_dups);
            }
        } else {  // GT
            if (index_col_id_ == 0) {
                seq_pages = stats_->num_pages_ * SELECTIVITY_GT;
            } else {
                random_pages = MACKERT_LOHMAN(stats_->num_pages_,
                                              stats_->num_distinct_values_[0]
                                              * SELECTIVITY_GT);
            }
        }
    }

    return (seq_pages + random_pages) * COST_DISK_READ_PAGE
           + random_pages * COST_DISK_SEEK_PAGE;
}

double IndexScan::estCardinality() const
{
    double card = stats_->num_distinct_values_[0];

    if (value_) {
        if (comp_op_ == EQ) {
            card /= stats_->num_distinct_values_[index_col_id_];
        } else {  // GT
            card *= SELECTIVITY_GT;
        }
    }

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
