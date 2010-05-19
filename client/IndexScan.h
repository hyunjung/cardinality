#ifndef CLIENT_INDEXSCAN_H_
#define CLIENT_INDEXSCAN_H_

#include <string>
#include <vector>
#include "client/Scan.h"
#include "lib/index/include/server.h"


namespace cardinality {

class IndexScan: public Scan {
public:
    IndexScan(const NodeID, const char *, const char *,
              const Table *, const PartStats *, const Query *,
              const char * = NULL);
    explicit IndexScan(google::protobuf::io::CodedInputStream *);
    IndexScan(const IndexScan &);
    ~IndexScan();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    void print(std::ostream &, const int) const;
#endif

    double estCost(const double = 0.0) const;
    double estCardinality() const;

protected:
    std::string index_col_;
    ValueType index_col_type_;
    CompOp comp_op_;
    Value *value_;
    ColID index_col_id_;

    Index *index_;
    std::vector<uint64_t> addrs_;
    std::size_t i_;

private:
    IndexScan& operator=(const IndexScan &);
};

}  // namespace cardinality

#endif  // CLIENT_INDEXSCAN_H_
