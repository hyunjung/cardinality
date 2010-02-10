#ifndef NLJOIN_H
#define NLJOIN_H

#include "Join.h"


namespace op {

class NLJoin: public Join {
public:
    NLJoin(const NodeID, boost::shared_ptr<Operator>, boost::shared_ptr<Scan>,
           const Query *, const int = -1, const char * = NULL);
    NLJoin();
    ~NLJoin();

    RC Open(const char * = NULL);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

protected:
    ColID idxJoinColID;
    static const ColID NOT_INDEX_JOIN = -1;

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

BOOST_SERIALIZATION_SHARED_PTR(op::NLJoin)

#endif
