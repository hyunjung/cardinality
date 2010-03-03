#ifndef CLIENT_OPTIMIZER_H_
#define CLIENT_OPTIMIZER_H_

#include "Operator.h"


#define COST_DISK_READ_PAGE 1.0
#define COST_DISK_SEEK_PAGE 0.5
#define COST_NET_XFER_BYTE 0.0025
#define SELECTIVITY_EQ 0.1
#define SELECTIVITY_GT 0.4

extern ca::Operator::Ptr buildQueryPlan(const Query *);

#endif  // CLIENT_OPTIMIZER_H_
