#include "client/Scan.h"


namespace cardinality {

Scan::Scan(const NodeID n, const char *f, const char *a,
           const Table *t, const PartitionStats *p, const Query *q)
    : Project(n),
      filename_(f),
      gteq_conds_(), join_conds_(),
      num_input_cols_(t->nbFields),
      alias_(a), table_(t), stats_(p), file_()
{
    initProject(q);
    initFilter(q);
}

Scan::Scan()
    : Project(),
      filename_(),
      gteq_conds_(), join_conds_(),
      num_input_cols_(),
      alias_(), table_(), stats_(), file_()
{
}

Scan::Scan(const Scan &x)
    : Project(x),
      filename_(x.filename_),
      gteq_conds_(x.gteq_conds_), join_conds_(x.join_conds_),
      num_input_cols_(x.num_input_cols_),
      alias_(x.alias_), table_(x.table_), stats_(x.stats_), file_()
{
}

Scan::~Scan()
{
}

void Scan::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbRestrictionsEqual; ++i) {
        if (hasCol(q->restrictionEqualFields[i])) {
            gteq_conds_.push_back(
                boost::make_tuple(&q->restrictionEqualValues[i],
                                  getInputColID(q->restrictionEqualFields[i]),
                                  EQ));
        }
    }

    for (int i = 0; i < q->nbRestrictionsGreaterThan; ++i) {
        if (hasCol(q->restrictionGreaterThanFields[i])) {
            gteq_conds_.push_back(
                boost::make_tuple(&q->restrictionGreaterThanValues[i],
                                  getInputColID(q->restrictionGreaterThanFields[i]),
                                  GT));
        }
    }

    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && hasCol(q->joinFields2[i])) {
            join_conds_.push_back(
                boost::make_tuple(getInputColID(q->joinFields1[i]),
                                  getInputColID(q->joinFields2[i]),
                                  table_->fieldsType[getInputColID(q->joinFields1[i])]));
        }
    }
}

const char * Scan::splitLine(const char *pos, Tuple &temp) const
{
    temp.clear();

    for (int i = 0; i < num_input_cols_ - 1; ++i) {
        const char *delim = static_cast<const char *>(rawmemchr(pos, '|'));
        temp.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    const char *delim = static_cast<const char *>(rawmemchr(pos, '\n'));
    temp.push_back(std::make_pair(pos, delim - pos));
    return delim + 1;
}

bool Scan::execFilter(const Tuple &tuple) const
{
    if (tuple.size() != static_cast<std::size_t>(num_input_cols_)) {
        return false;
    }

    for (std::size_t i = 0; i < gteq_conds_.size(); ++i) {
        if (gteq_conds_[i].get<0>()->type == INT) {
            int cmp = gteq_conds_[i].get<0>()->intVal
                      - parseInt(tuple[gteq_conds_[i].get<1>()].first,
                                 tuple[gteq_conds_[i].get<1>()].second);
            if ((gteq_conds_[i].get<2>() == EQ && cmp != 0)
                || (gteq_conds_[i].get<2>() == GT && cmp >= 0)) {
                return false;
            }
        } else {  // STRING
            if (gteq_conds_[i].get<2>() == EQ) {
                if ((gteq_conds_[i].get<0>()->intVal
                     != tuple[gteq_conds_[i].get<1>()].second)
                    || (std::memcmp(gteq_conds_[i].get<0>()->charVal,
                                    tuple[gteq_conds_[i].get<1>()].first,
                                    tuple[gteq_conds_[i].get<1>()].second))) {
                    return false;
                }
            } else {  // GT
                int cmp = std::memcmp(gteq_conds_[i].get<0>()->charVal,
                                      tuple[gteq_conds_[i].get<1>()].first,
                                      std::min(gteq_conds_[i].get<0>()->intVal,
                                               tuple[gteq_conds_[i].get<1>()].second));
                if (cmp > 0
                    || (cmp == 0
                        && gteq_conds_[i].get<0>()->intVal
                           >= tuple[gteq_conds_[i].get<1>()].second)) {
                    return false;
                }
            }
        }
    }

    for (std::size_t i = 0; i < join_conds_.size(); ++i) {
        if (!join_conds_[i].get<2>()) {  // INT
            if (parseInt(tuple[join_conds_[i].get<0>()].first,
                         tuple[join_conds_[i].get<0>()].second)
                != parseInt(tuple[join_conds_[i].get<1>()].first,
                            tuple[join_conds_[i].get<1>()].second)) {
                return false;
            }
        } else {  // STRING
            if ((tuple[join_conds_[i].get<0>()].second
                 != tuple[join_conds_[i].get<1>()].second)
                || (std::memcmp(tuple[join_conds_[i].get<0>()].first,
                                tuple[join_conds_[i].get<1>()].first,
                                tuple[join_conds_[i].get<1>()].second))) {
                return false;
            }
        }
    }

    return true;
}

void Scan::execProject(const Tuple &input_tuple, Tuple &output_tuple) const
{
    output_tuple.clear();

    for (std::size_t i = 0; i < selected_input_col_ids_.size(); ++i) {
        output_tuple.push_back(input_tuple[selected_input_col_ids_[i]]);
    }
}

bool Scan::hasCol(const char *col) const
{
    return (col[alias_.size()] == '.' || col[alias_.size()] == '\0')
           && !std::memcmp(col, alias_.data(), alias_.size());
}

ColID Scan::getInputColID(const char *col) const
{
    const char *dot = std::strchr(col, '.');
    if (dot == NULL) {
        throw std::runtime_error("invalid column name");
    }

    for (int i = 0; i < table_->nbFields; ++i) {
        if (!std::strcmp(dot + 1, table_->fieldsName[i])) {
            return i;
        }
    }
    throw std::runtime_error("column name not found");
}

ColID Scan::getBaseColID(const ColID cid) const
{
    return selected_input_col_ids_[cid];
}

ValueType Scan::getColType(const char *col) const
{
    return table_->fieldsType[getInputColID(col)];
}

double Scan::estTupleLength() const
{
    double length = 0;
    for (ColID i = 0; i < numOutputCols(); ++i) {
        length += estColLength(i);
    }

    return length;
}

double Scan::estColLength(const ColID cid) const
{
    return stats_->col_lengths_[selected_input_col_ids_[cid]];
}

}  // namespace cardinality
