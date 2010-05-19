#ifndef CLIENT_SEQSCAN_H_
#define CLIENT_SEQSCAN_H_

#include "client/Scan.h"


namespace cardinality {

class SeqScan: public Scan {
public:
    SeqScan(const NodeID, const char *, const char *,
            const Table *, const PartStats *, const Query *);
    explicit SeqScan(google::protobuf::io::CodedInputStream *);
    SeqScan(const SeqScan &);
    ~SeqScan();
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
    const char *pos_;

private:
    SeqScan& operator=(const SeqScan &);
};

}  // namespace cardinality

#endif  // CLIENT_SEQSCAN_H_
