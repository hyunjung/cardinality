#ifndef CLIENT_INDEXSCAN_H_
#define CLIENT_INDEXSCAN_H_

#include "client/Scan.h"
#include "lib/index/include/server.h"


namespace cardinality {

class IndexScan: public Scan {
public:
    typedef boost::shared_ptr<IndexScan> Ptr;

    IndexScan(const NodeID, const char *, const char *,
              const Table *, const PartitionStats *, const Query *,
              const char * = NULL);
    ~IndexScan();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    void print(std::ostream &, const int) const;

    double estCost(const double = 0.0) const;
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

}  // namespace cardinality

BOOST_SERIALIZATION_SHARED_PTR(cardinality::IndexScan)

#endif  // CLIENT_INDEXSCAN_H_
