#include <cstring>
#include "Scan.h"

using namespace op;


Scan::Scan(const NodeID n, const char *f, const char *a, const Table *t, const Query *q)
    : Operator(n), fileName(f), gteqConds(), joinConds(),
      alias(a), table(t), file(), lineBuffer(new char[4096])
{
    initProject(q);
    initFilter(q);
}

Scan::Scan()
    : fileName(), gteqConds(), joinConds(),
      alias(), table(NULL), file(), lineBuffer(new char[4096])
{
}

Scan::~Scan()
{
}

void Scan::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbRestrictionsEqual; ++i) {
        if (hasCol(q->restrictionEqualFields[i])) {
            gteqConds.push_back(boost::make_tuple(getInputColID(q->restrictionEqualFields[i]),
                                                  &q->restrictionEqualValues[i],
                                                  EQ));
        }
    }

    for (int i = 0; i < q->nbRestrictionsGreaterThan; ++i) {
        if (hasCol(q->restrictionGreaterThanFields[i])) {
            gteqConds.push_back(boost::make_tuple(getInputColID(q->restrictionGreaterThanFields[i]),
                                                  &q->restrictionGreaterThanValues[i],
                                                  GT));
        }
    }

    for (int i = 0; i < q->nbJoins; ++i) {
        if (hasCol(q->joinFields1[i]) && hasCol(q->joinFields2[i])) {
            joinConds.push_back(boost::make_tuple(getInputColID(q->joinFields1[i]),
                                                  getInputColID(q->joinFields2[i]),
                                                  table->fieldsType[getInputColID(q->joinFields1[i])]));
        }
    }
}

const char * Scan::splitLine(const char *pos, char *buf, Tuple &temp) const
{
    const char *eol = strchr(pos, '\n');
    if (pos == eol) {
        throw std::runtime_error("invalid file");
    }
    memcpy(buf, pos, eol - pos);
    buf[eol - pos] = '\0';

    char *c = buf;
    temp.clear();
    while (true) {
        temp.push_back(c);
        if (!(c = strchr(c + 1, '|'))) {
            break;
        }
        *c++ = 0;
    }

    return eol + 1;
}

bool Scan::execFilter(Tuple &tuple) const
{
    for (size_t i = 0; i < gteqConds.size(); ++i) {
        int cmp = (gteqConds[i].get<1>()->type == INT) ?
                  (gteqConds[i].get<1>()->intVal - static_cast<uint32_t>(atoi(tuple[gteqConds[i].get<0>()]))) :
                  (strcmp(gteqConds[i].get<1>()->charVal, tuple[gteqConds[i].get<0>()]));

        switch (gteqConds[i].get<2>()) {
        case EQ:
            if (cmp != 0) {
                return false;
            }
            break;

        case GT:
            if (cmp >= 0) {
                return false;
            }
            break;
        }
    }

    for (size_t i = 0; i < joinConds.size(); ++i) {
        if ((joinConds[i].get<2>() == INT
             && atoi(tuple[joinConds[i].get<0>()]) != atoi(tuple[joinConds[i].get<1>()]))
            || (joinConds[i].get<2>() == STRING
                && strcmp(tuple[joinConds[i].get<0>()], tuple[joinConds[i].get<1>()]) != 0)) {
            return false;
        }
    }

    return true;
}

void Scan::execProject(Tuple &inputTuple, Tuple &outputTuple) const
{
    outputTuple.clear();

    for (size_t i = 0; i < selectedInputColIDs.size(); ++i) {
        outputTuple.push_back(inputTuple[selectedInputColIDs[i]]);
    }
}

bool Scan::hasCol(const char *col) const
{
    return col[alias.size()] == '.' && memcmp(col, alias.data(), alias.size()) == 0;
}

ColID Scan::getInputColID(const char *col) const
{
    const char *dot = strchr(col, '.');
    if (dot == NULL) {
        throw std::runtime_error("invalid column name");
    }

    for (int i = 0; i < table->nbFields; ++i) {
        if (!strcmp(dot + 1, table->fieldsName[i])) {
            return i;
        }
    }
    throw std::runtime_error("column name not found");
}

ValueType Scan::getColType(const char *col) const
{
    return table->fieldsType[getInputColID(col)];
}
