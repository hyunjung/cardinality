#ifndef CLIENT_UTIL_H_
#define CLIENT_UTIL_H_

#include <cstring>
#include "include/client.h"


namespace cardinality {

static int compareValue(const Value *a, const Value *b)
{
    if (a->type == INT) {
        return a->intVal - b->intVal;
    } else {  // STRING
        int cmp = std::memcmp(a->charVal, b->charVal,
                              std::min(a->intVal, b->intVal));
        if (cmp == 0) {
            cmp = a->intVal - b->intVal;
        }
        return cmp;
    }
}

struct HashQuery {
    std::size_t operator()(const Query *q) const {
        return (q->nbTable << 20)
               + (q->nbOutputFields << 12)
               + (q->nbJoins << 8)
               + (q->nbRestrictionsEqual << 4)
               + (q->nbRestrictionsGreaterThan);
    }
};

struct EqualToQuery {
    bool operator()(const Query *a, const Query *b) const {
        if (a->nbTable != b->nbTable
            || a->nbOutputFields != b->nbOutputFields
            || a->nbJoins != b->nbJoins
            || a->nbRestrictionsEqual != b->nbRestrictionsEqual
            || a->nbRestrictionsGreaterThan != b->nbRestrictionsGreaterThan) {
            return false;
        }

        for (int i = 0; i < a->nbTable; ++i) {
            if (std::strcmp(a->tableNames[i], b->tableNames[i])
                || std::strcmp(a->aliasNames[i], b->aliasNames[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbOutputFields; ++i) {
            if (std::strcmp(a->outputFields[i], b->outputFields[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbJoins; ++i) {
            if (std::strcmp(a->joinFields1[i], b->joinFields1[i])
                || std::strcmp(a->joinFields2[i], b->joinFields2[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbRestrictionsEqual; ++i) {
            if (std::strcmp(a->restrictionEqualFields[i],
                            b->restrictionEqualFields[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbRestrictionsGreaterThan; ++i) {
            if (std::strcmp(a->restrictionGreaterThanFields[i],
                            b->restrictionGreaterThanFields[i])) {
                return false;
            }
        }

        return true;
    }
};

}  // namespace cardinality

#endif  // CLIENT_UTIL_H_
