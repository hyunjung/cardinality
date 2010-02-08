#ifndef INDEXSCAN_H
#define INDEXSCAN_H

#include "Scan.h"
#include "../lib/index/include/server.h"


namespace op {

class IndexScan: public Scan {
public:
    IndexScan(const NodeID, const char *, const char *, const Table *, const Query *);
    IndexScan();
    ~IndexScan();

    RC Open();
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

protected:
    std::string indexCol;
    CompOp compOp;
    Value *value;

    Index *index;
    TxnState *txn;
    Record record;
    bool getNotCalled;
    bool checkIndexCond;

private:
    IndexScan(const IndexScan &);
    IndexScan& operator=(const IndexScan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Scan>(*this);
        ar & indexCol;
        ar & compOp;
        ar & value;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(op::IndexScan)

#endif
