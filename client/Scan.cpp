#include "Scan.h"

using namespace ca;


Scan::Scan(const NodeID n, const char *f, const char *a,
           const Table *t, const PartitionStats *p, const Query *q)
    : Operator(n), fileName(f), gteqConds(), joinConds(), numInputCols(t->nbFields),
      alias(a), table(t), stats(p), file()
{
    initProject(q);
    initFilter(q);
}

Scan::Scan()
    : fileName(), gteqConds(), joinConds(), numInputCols(),
      alias(), table(NULL), stats(NULL), file()
{
}

Scan::~Scan()
{
}

void Scan::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbRestrictionsEqual; ++i) {
        if (hasCol(q->restrictionEqualFields[i])) {
            gteqConds.push_back(
                boost::make_tuple(getInputColID(q->restrictionEqualFields[i]),
                                  &q->restrictionEqualValues[i],
                                  EQ));
        }
    }

    for (int i = 0; i < q->nbRestrictionsGreaterThan; ++i) {
        if (hasCol(q->restrictionGreaterThanFields[i])) {
            gteqConds.push_back(
                boost::make_tuple(getInputColID(q->restrictionGreaterThanFields[i]),
                                  &q->restrictionGreaterThanValues[i],
                                  GT));
        }
    }

    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && hasCol(q->joinFields2[i])) {
            joinConds.push_back(
                boost::make_tuple(getInputColID(q->joinFields1[i]),
                                  getInputColID(q->joinFields2[i]),
                                  table->fieldsType[getInputColID(q->joinFields1[i])]));
        }
    }
}

const char * Scan::splitLine(const char *pos, const char *eof, Tuple &temp) const
{
    temp.clear();

    for (int i = 0; i < numInputCols - 1; ++i) {
        const char *delim = static_cast<const char *>(std::memchr(pos, '|', eof - pos));
        temp.push_back(std::make_pair(pos, delim - pos));
        pos = delim + 1;
    }

    const char *delim = static_cast<const char *>(std::memchr(pos, '\n', eof - pos));
    temp.push_back(std::make_pair(pos, delim - pos));
    return delim + 1;
}

bool Scan::execFilter(const Tuple &tuple) const
{
    if (tuple.size() != static_cast<size_t>(numInputCols)) {
        return false;
    }

    for (size_t i = 0; i < gteqConds.size(); ++i) {
        if (gteqConds[i].get<1>()->type == INT) {
            int cmp = gteqConds[i].get<1>()->intVal
                      - parseInt(tuple[gteqConds[i].get<0>()].first,
                                 tuple[gteqConds[i].get<0>()].second);
            if ((gteqConds[i].get<2>() == EQ && cmp != 0)
                || (gteqConds[i].get<2>() == GT && cmp >= 0)) {
                return false;
            }
        } else { // STRING
            if (gteqConds[i].get<2>() == EQ) {
                if ((gteqConds[i].get<1>()->intVal
                     != tuple[gteqConds[i].get<0>()].second)
                    || (std::memcmp(gteqConds[i].get<1>()->charVal,
                                    tuple[gteqConds[i].get<0>()].first,
                                    tuple[gteqConds[i].get<0>()].second))) {
                    return false;
                }
            } else { // GT
                int cmp = std::memcmp(gteqConds[i].get<1>()->charVal,
                                      tuple[gteqConds[i].get<0>()].first,
                                      std::min(gteqConds[i].get<1>()->intVal,
                                               tuple[gteqConds[i].get<0>()].second));
                if (cmp > 0
                    || (cmp == 0 && gteqConds[i].get<1>()->intVal >= tuple[gteqConds[i].get<0>()].second)) {
                    return false;
                }
            }
        }
    }

    for (size_t i = 0; i < joinConds.size(); ++i) {
        if (joinConds[i].get<2>() == INT) {
            if (parseInt(tuple[joinConds[i].get<0>()].first,
                         tuple[joinConds[i].get<0>()].second)
                != parseInt(tuple[joinConds[i].get<1>()].first,
                            tuple[joinConds[i].get<1>()].second)) {
                return false;
            }
        } else { // STRING
            if ((tuple[joinConds[i].get<0>()].second
                 != tuple[joinConds[i].get<1>()].second)
                || (std::memcmp(tuple[joinConds[i].get<0>()].first,
                                tuple[joinConds[i].get<1>()].first,
                                tuple[joinConds[i].get<1>()].second))) {
                return false;
            }
        }
    }

    return true;
}

void Scan::execProject(const Tuple &inputTuple, Tuple &outputTuple) const
{
    outputTuple.clear();

    for (size_t i = 0; i < selectedInputColIDs.size(); ++i) {
        outputTuple.push_back(inputTuple[selectedInputColIDs[i]]);
    }
}

bool Scan::hasCol(const char *col) const
{
    return (col[alias.size()] == '.' || col[alias.size()] == '\0')
           && !std::memcmp(col, alias.data(), alias.size());
}

ColID Scan::getInputColID(const char *col) const
{
    const char *dot = std::strchr(col, '.');
    if (dot == NULL) {
        throw std::runtime_error("invalid column name");
    }

    for (int i = 0; i < table->nbFields; ++i) {
        if (!std::strcmp(dot + 1, table->fieldsName[i])) {
            return i;
        }
    }
    throw std::runtime_error("column name not found");
}

ValueType Scan::getColType(const char *col) const
{
    return table->fieldsType[getInputColID(col)];
}

double Scan::estTupleLength() const
{
    double length = 0;
    for (size_t i = 0; i < numOutputCols(); ++i) {
        length += estColLength(i);
    }

    return length;
}

double Scan::estColLength(const ColID cid) const
{
    return stats->colLengths[selectedInputColIDs[cid]];
}
