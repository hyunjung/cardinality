#ifndef CLIENT_OPTIMIZER_H_
#define CLIENT_OPTIMIZER_H_

#include "Operator.h"


extern ca::Operator::Ptr buildQueryPlan(const Query *);

#endif  // CLIENT_OPTIMIZER_H_
