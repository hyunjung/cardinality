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

}  // namespace cardinality

#endif  // CLIENT_UTIL_H_
