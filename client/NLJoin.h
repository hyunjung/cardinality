#ifndef CLIENT_NLJOIN_H_
#define CLIENT_NLJOIN_H_

#include "client/Join.h"


namespace cardinality {

class NLJoin: public Join {
public:
    NLJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *, const int = -1, const char * = NULL);
    explicit NLJoin(google::protobuf::io::CodedInputStream *);
    NLJoin(const NLJoin &);
    ~NLJoin();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    void print(std::ostream &, const int) const;

    double estCost(const double = 0.0) const;

protected:
    ColID index_join_col_id_;
    static const ColID NOT_INDEX_JOIN = 0xffff;

    enum { RIGHT_OPEN, RIGHT_REOPEN, RIGHT_GETNEXT } state_;
    Tuple left_tuple_;
    Tuple right_tuple_;

private:
    NLJoin& operator=(const NLJoin &);
};

}  // namespace cardinality

#endif  // CLIENT_NLJOIN_H_
