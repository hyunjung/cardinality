#ifndef NLJOIN_H
#define NLJOIN_H

#include "Join.h"


namespace ca {

class NLJoin: public Join {
public:
    typedef boost::shared_ptr<NLJoin> Ptr;

    NLJoin(const NodeID, Operator::Ptr, Scan::Ptr,
           const Query *, const int = -1, const char * = NULL);
    NLJoin();
    ~NLJoin();

    RC Open(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

    double estCost() const;

protected:
    ColID idxJoinColID;
    static const ColID NOT_INDEX_JOIN = -1;

    enum { RIGHT_OPEN, RIGHT_RESCAN, RIGHT_GETNEXT } state;
    Tuple leftTuple;

private:
    NLJoin(const NLJoin &);
    NLJoin& operator=(const NLJoin &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Join>(*this);
        ar & idxJoinColID;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::NLJoin)

#endif
