#ifndef CLIENT_INDEXSCAN_H_
#define CLIENT_INDEXSCAN_H_

#include "client/Scan.h"
#include "lib/index/include/server.h"


namespace cardinality {

class IndexScan: public Scan {
public:
    IndexScan(const NodeID, const char *, const char *,
              const Table *, const PartitionStats *, const Query *,
              const char * = NULL);
    IndexScan();
    IndexScan(const IndexScan &);
    ~IndexScan();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    void Serialize(google::protobuf::io::CodedOutputStream *) const;
    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    void print(std::ostream &, const int) const;

    double estCost(const double = 0.0) const;
    double estCardinality() const;

protected:
    std::string index_col_;
    ValueType index_col_type_;
    CompOp comp_op_;
    Value *value_;
    bool unique_;

    Index *index_;
    std::vector<uint64_t> addrs_;
    std::size_t i_;

private:
    IndexScan& operator=(const IndexScan &);
};

}  // namespace cardinality

#endif  // CLIENT_INDEXSCAN_H_
