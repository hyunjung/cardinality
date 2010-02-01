#include <stdexcept>
#include <cstring>
#include "Scan.h"

using namespace op;


Scan::Scan(const Query *q, const char *_alias, const Table *_table, const char *_fileName)
    : alias(_alias), lenAlias(strlen(_alias)), table(_table), fileName(_fileName),
      ifs(), lineBuffer(new char[4096]), eqConds(), gtConds(), joinConds()
{
    initProject(q);
    initFilter(q);
}

Scan::~Scan()
{
}

void Scan::initFilter(const Query *q)
{
    for (int i = 0; i < q->nbRestrictionsEqual; ++i) {
        if (hasCol(q->restrictionEqualFields[i])) {
            eqConds.push_back(std::make_pair(getInputColID(q->restrictionEqualFields[i]),
                                             &q->restrictionEqualValues[i]));
        }
    }

    for (int i = 0; i < q->nbRestrictionsGreaterThan; ++i) {
        if (hasCol(q->restrictionGreaterThanFields[i])) {
            gtConds.push_back(std::make_pair(getInputColID(q->restrictionGreaterThanFields[i]),
                                             &q->restrictionGreaterThanValues[i]));
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

bool Scan::execFilter(Tuple &tuple) const
{
    for (size_t i = 0; i < eqConds.size(); ++i) {
        if ((eqConds[i].second->type == INT
             && eqConds[i].second->intVal != static_cast<uint32_t>(atoi(tuple[eqConds[i].first])))
            || (eqConds[i].second->type == STRING
                && strcmp(eqConds[i].second->charVal, tuple[eqConds[i].first]) != 0)) {
            return false;
        }
    }

    for (size_t i = 0; i < gtConds.size(); ++i) {
        if ((gtConds[i].second->type == INT
             && gtConds[i].second->intVal >= static_cast<uint32_t>(atoi(tuple[gtConds[i].first])))
            || (gtConds[i].second->type == STRING
                && strcmp(gtConds[i].second->charVal, tuple[gtConds[i].first]) >= 0)) {
            return false;
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
    return col[lenAlias] == '.' && memcmp(col, alias, lenAlias) == 0;
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
