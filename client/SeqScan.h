#ifndef SEQSCAN_H
#define SEQSCAN_H

#include "Scan.h"


namespace op {

class SeqScan: public Scan {
public:
    SeqScan(const Query *, const char *, const Table *, const char *);
    ~SeqScan();

    RC open();
    RC getNext(Tuple &);
    RC close();

    void print(std::ostream &, const int) const;

private:
    SeqScan(const SeqScan &);
    SeqScan& operator=(const SeqScan &);
};

}

#endif
