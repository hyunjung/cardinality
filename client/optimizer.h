#ifndef CLIENT_OPTIMIZER_H_
#define CLIENT_OPTIMIZER_H_

#include "Operator.h"


extern ca::Operator::Ptr buildQueryPlanIgnoringPartitions(const Query *);
extern ca::Operator::Ptr buildSimpleQueryPlanIgnoringPartitions(const Query *);
extern ca::Operator::Ptr buildSimpleQueryPlanForSingleTable(const Query *);
extern ca::Operator::Ptr buildSimpleQueryPlan(const Query *);

#endif  // CLIENT_OPTIMIZER_H_
