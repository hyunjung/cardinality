#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../include/client.h"
#include "Operator.h"

extern op::Operator::Ptr buildQueryPlan(const Query *);

#endif
