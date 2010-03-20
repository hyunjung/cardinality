#include "client/IndexScan.h"


namespace cardinality {

IndexScan::IndexScan(const NodeID n, const char *f, const char *a,
                     const Table *t, const PartitionStats *p, const Query *q,
                     const char *col)
    : Scan(n, f, a, t, p, q),
      index_col_(), index_col_type_(),
      comp_op_(EQ), value_(NULL), unique_(),
      index_(), txn_(), record_(),
      state_(), check_index_cond_(),
      key_intval_(), key_charval_()
{
    if (col) {  // NLIJ
        const char *dot = std::strchr(col, '.');
        if (dot == NULL) {
            throw std::runtime_error("invalid column name");
        }
        index_col_ = table_->tableName;
        index_col_ += '.';
        index_col_ += dot + 1;
        index_col_type_ = getColType(col);

        // equivalent to unique_ = getInputColID(t->fieldsName[0]) == 0
        unique_ = std::strcmp(t->fieldsName[0], dot + 1) == 0;
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
                unique_ = gteq_conds_[i].get<1>() == 0;
                gteq_conds_.erase(gteq_conds_.begin() + i);
                break;
            }
        }

        if (index_col_.empty()) {
            throw std::runtime_error("index_ed column not found");
        }
    }
}

IndexScan::IndexScan()
    : Scan(),
      index_col_(), index_col_type_(),
      comp_op_(), value_(), unique_(),
      index_(), txn_(), record_(),
      state_(), check_index_cond_(),
      key_intval_(), key_charval_()
{
}

IndexScan::IndexScan(const IndexScan &x)
    : Scan(x),
      index_col_(x.index_col_), index_col_type_(x.index_col_type_),
      comp_op_(x.comp_op_), value_(x.value_), unique_(x.unique_),
      index_(), txn_(), record_(),
      state_(), check_index_cond_(),
      key_intval_(), key_charval_()
{
}

IndexScan::~IndexScan()
{
}

Operator::Ptr IndexScan::clone() const
{
    return Operator::Ptr(new IndexScan(*this));
}

void IndexScan::Open(const char *left_ptr, const uint32_t left_len)
{
    file_.open(filename_);
    openIndex(index_col_.c_str(), &index_);

    record_.val.type = index_col_type_;
    if (index_col_type_ == INT) {
        if (left_ptr) {
            key_intval_ = parseInt(left_ptr, left_len);
        } else {
            key_intval_ = value_->intVal;
        }
        record_.val.intVal = key_intval_;
    } else {  // STRING
        if (left_ptr) {
            key_charval_ = left_ptr;
            key_intval_ = left_len;
        } else {
            key_charval_ = value_->charVal;
            key_intval_ = value_->intVal;
        }
        std::memcpy(record_.val.charVal, key_charval_, key_intval_);
        record_.val.charVal[key_intval_] = '\0';
    }

    if (!(unique_ && comp_op_ == EQ)) {
        beginTransaction(&txn_);
    }
    state_ = INDEX_GET;
    check_index_cond_ = true;
}

void IndexScan::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    if (index_col_type_ == INT) {
        if (left_ptr) {
            key_intval_ = parseInt(left_ptr, left_len);
        }
        record_.val.intVal = key_intval_;
    } else {  // STRING
        if (left_ptr) {
            key_charval_ = left_ptr;
            key_intval_ = left_len;
        }
        std::memcpy(record_.val.charVal, key_charval_, key_intval_);
        record_.val.charVal[key_intval_] = '\0';
    }

    state_ = INDEX_GET;
    check_index_cond_ = true;
}

bool IndexScan::GetNext(Tuple &tuple)
{
    ErrCode ec;
    Tuple temp;

    switch (state_) {
    case INDEX_DONE:
        return true;

    case INDEX_GET:
        if (unique_ && comp_op_ == EQ) {
            ec = get(index_, NULL, &record_);
            if (ec == SUCCESS) {
                state_ = INDEX_DONE;

                splitLine(file_.begin() + record_.address, temp);

                if (execFilter(temp)) {
                    execProject(temp, tuple);
                    return false;
                }
            }
            return true;
        }

        ec = get(index_, txn_, &record_);
        state_ = INDEX_GETNEXT;

        // GT: the first tuple will be returned in the while loop
        switch (ec) {
        case SUCCESS:
            if (comp_op_ == EQ) {
                splitLine(file_.begin() + record_.address, temp);

                if (execFilter(temp)) {
                    execProject(temp, tuple);
                    return false;
                }
            }
            break;

        case KEY_NOTFOUND:
            if (comp_op_ == EQ) {
                return true;
            } else {  // GT
                check_index_cond_ = false;
            }
            break;

        default:
            break;
        }

    case INDEX_GETNEXT:
        while ((ec = getNext(index_, txn_, &record_)) == SUCCESS) {
            if (check_index_cond_) {
                if (index_col_type_ == INT) {
                    if (comp_op_ == EQ) {
                        if (key_intval_ != record_.val.intVal) {
                            return true;
                        }
                    } else {  // GT
                        if (key_intval_ >= record_.val.intVal) {
                            continue;
                        }
                        check_index_cond_ = false;
                    }
                } else {  // STRING
                    if (comp_op_ == EQ) {
                        if (record_.val.charVal[key_intval_] != '\0'
                            || std::memcmp(key_charval_, record_.val.charVal, key_intval_)) {
                            return true;
                        }
                    } else {  // GT
                        uint32_t charval_len = std::strlen(record_.val.charVal);
                        int cmp = std::memcmp(key_charval_, record_.val.charVal,
                                              std::min(key_intval_, charval_len));
                        if (cmp > 0 || (cmp == 0 && key_intval_ >= charval_len)) {
                            continue;
                        }
                        check_index_cond_ = false;
                    }
                }
            }

            splitLine(file_.begin() + record_.address, temp);

            if (execFilter(temp)) {
                execProject(temp, tuple);
                return false;
            }
        }
        break;
    }

    return true;
}

void IndexScan::Close()
{
    if (!(unique_ && comp_op_ == EQ)) {
        commitTransaction(txn_);
    }
    closeIndex(index_);
    file_.close();
}

void IndexScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "IndexScan@" << node_id() << " " << filename_ << " ";
    if (unique_) {
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
        random_pages = MACKERT_LOHMAN(stats_->num_pages_,
                                      (unique_) ? 1.0 : 3.0, left_cardinality)
                       / left_cardinality;
    } else {
        if (comp_op_ == EQ) {
            if (unique_) {
                seq_pages = MACKERT_LOHMAN(stats_->num_pages_, 1.0);
            } else {
                random_pages = MACKERT_LOHMAN(stats_->num_pages_, 3.0);
            }
        } else {  // GT
            if (unique_) {
                seq_pages = stats_->num_pages_ * SELECTIVITY_GT;
            } else {
                random_pages = MACKERT_LOHMAN(stats_->num_pages_,
                                              stats_->cardinality_ * SELECTIVITY_GT);
            }
        }
    }

    return (seq_pages + random_pages) * COST_DISK_READ_PAGE
           + random_pages * COST_DISK_SEEK_PAGE;
}

double IndexScan::estCardinality() const
{
    double card;

    if (comp_op_ == EQ) {
        if (unique_) {
            card = 1.0;
        } else {
            card = SELECTIVITY_EQ * stats_->cardinality_;
        }
    } else {  // GT
        card = SELECTIVITY_GT * stats_->cardinality_;
    }

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
