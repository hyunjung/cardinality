#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../include/client.h"
#include "Operator.h"

#define PAGE_SIZE 4096

#define COST_DISK_READ_PAGE 1.0
#define COST_DISK_SEEK_PAGE 0.5
#define COST_NET_XFER_BYTE 0.0025
#define SELECTIVITY_EQ 0.1
#define SELECTIVITY_GT 0.4

struct PartitionStats {
    PartitionStats(const int n) {
        colLengths = new double[n];
        for (int i = 0; i < n; ++i) {
            colLengths[i] = 0;
        }
    }

    ~PartitionStats() {
        delete [] colLengths;
    }

    size_t numPages;
    double cardinality;
    double *colLengths;
};


extern PartitionStats *sampleTable(const std::string, const int);
extern ca::Operator::Ptr buildQueryPlan(const Query *);

#endif
