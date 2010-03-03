#ifndef NLJOIN_H
#define NLJOIN_H

#include "Join.h"


namespace ca {

class NLJoin: public Join {
public:
    typedef boost::shared_ptr<NLJoin> Ptr;

    NLJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *, const int = -1, const char * = NULL);
    ~NLJoin();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

    double estCost() const;

protected:
    NLJoin();
    NLJoin(const NLJoin &);

    ColID index_join_col_id_;
    static const ColID NOT_INDEX_JOIN = -1;

    enum { RIGHT_OPEN, RIGHT_REOPEN, RIGHT_GETNEXT } state_;
    Tuple left_tuple_;

private:
    NLJoin& operator=(const NLJoin &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Join>(*this);
        ar & index_join_col_id_;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::NLJoin)

#endif
