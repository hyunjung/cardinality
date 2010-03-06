#ifndef CLIENT_OPTIMIZER_H_
#define CLIENT_OPTIMIZER_H_

#include "client/Operator.h"


extern cardinality::Operator::Ptr buildQueryPlanIgnoringPartitions(const Query *);
extern cardinality::Operator::Ptr buildSimpleQueryPlanIgnoringPartitions(const Query *);
extern cardinality::Operator::Ptr buildSimpleQueryPlanForSingleTable(const Query *);
extern cardinality::Operator::Ptr buildSimpleQueryPlan(const Query *);

#endif  // CLIENT_OPTIMIZER_H_
