#ifndef INDEXSCAN_H
#define INDEXSCAN_H

#include "Scan.h"


namespace op {

class IndexScan: public Scan {
public:
    IndexScan(const Query *, const char *, const Table *, const char *);
    ~IndexScan();

    RC Open();
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

private:
    IndexScan(const IndexScan &);
    IndexScan& operator=(const IndexScan &);
};

}

#endif
