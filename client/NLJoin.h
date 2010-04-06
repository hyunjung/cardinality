#ifndef CLIENT_NLJOIN_H_
#define CLIENT_NLJOIN_H_

#include "client/Join.h"


namespace cardinality {

class NLJoin: public Join {
public:
    NLJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *, const int = -1, const char * = NULL);
    ~NLJoin();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    void print(std::ostream &, const int) const;

    double estCost(const double = 0.0) const;

protected:
    NLJoin();
    NLJoin(const NLJoin &);

    ColID index_join_col_id_;
    static const ColID NOT_INDEX_JOIN = -1;

    enum { RIGHT_OPEN, RIGHT_REOPEN, RIGHT_GETNEXT } state_;
    Tuple left_tuple_;
    Tuple right_tuple_;

private:
    NLJoin& operator=(const NLJoin &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Join>(*this);
        ar & index_join_col_id_;
    }
};

}  // namespace cardinality

#endif  // CLIENT_NLJOIN_H_
