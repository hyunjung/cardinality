#ifndef INDEXSCAN_H
#define INDEXSCAN_H

#include "Scan.h"
#include "../lib/index/include/server.h"


namespace op {

class IndexScan: public Scan {
public:
    typedef boost::shared_ptr<IndexScan> Ptr;

    IndexScan(const NodeID, const char *, const char *,
              const Table *, const Query *, const char * = NULL);
    IndexScan();
    ~IndexScan();

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReScan(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

protected:
    std::string indexCol;
    ValueType indexColType;
    CompOp compOp;
    Value *value;
    bool unique;

    Index *index;
    TxnState *txn;
    Record record;
    enum { INDEX_GET, INDEX_DONE, INDEX_GETNEXT } state;
    bool checkIndexCond;
    uint32_t keyIntVal;
    const char *keyCharVal;

private:
    IndexScan(const IndexScan &);
    IndexScan& operator=(const IndexScan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Scan>(*this);
        ar & indexCol;
        ar & indexColType;
        ar & compOp;
        ar & value;
        ar & unique;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(op::IndexScan)

#endif
