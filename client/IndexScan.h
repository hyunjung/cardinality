#ifndef INDEXSCAN_H
#define INDEXSCAN_H

#include "Scan.h"
#include "../lib/index/include/server.h"


namespace ca {

class IndexScan: public Scan {
public:
    typedef boost::shared_ptr<IndexScan> Ptr;

    IndexScan(const NodeID, const char *, const char *,
              const Table *, const PartitionStats *, const Query *,
              const char * = NULL, const double = 0);
    ~IndexScan();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

    double estCost() const;
    double estCardinality() const;

protected:
    IndexScan();
    IndexScan(const IndexScan &);

    std::string index_col_;
    ValueType index_col_type_;
    CompOp comp_op_;
    Value *value_;
    bool unique_;

    Index *index_;
    TxnState *txn_;
    Record record_;
    enum { INDEX_GET, INDEX_DONE, INDEX_GETNEXT } state_;
    bool check_index_cond_;
    uint32_t key_intval_;
    const char *key_charval_;
    const double outer_cardinality_;

private:
    IndexScan& operator=(const IndexScan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Scan>(*this);
        ar & index_col_;
        ar & index_col_type_;
        ar & comp_op_;
        ar & value_;
        ar & unique_;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::IndexScan)

#endif
