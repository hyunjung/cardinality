#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../include/client.h"
#include "Operator.h"

#define PAGE_SIZE 4096


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

    size_t fileSize;
    double tupleLength;
    double *colLengths;
};


extern PartitionStats *sampleTable(const std::string, const int);
extern op::Operator::Ptr buildQueryPlan(const Query *);

#endif
