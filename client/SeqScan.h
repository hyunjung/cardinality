#ifndef CLIENT_SEQSCAN_H_
#define CLIENT_SEQSCAN_H_

#include "client/Scan.h"


namespace cardinality {

class SeqScan: public Scan {
public:
    SeqScan(const NodeID, const char *, const char *,
            const Table *, const PartitionStats *, const Query *);
    SeqScan();
    SeqScan(const SeqScan &);
    ~SeqScan();
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
    const char *pos_;

private:
    SeqScan& operator=(const SeqScan &);
};

}  // namespace cardinality

#endif  // CLIENT_SEQSCAN_H_
