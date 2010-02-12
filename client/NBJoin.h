#ifndef NBJOIN_H
#define NBJOIN_H

#include <boost/shared_array.hpp>
#include "Join.h"


namespace op {

class NBJoin: public Join {
public:
    NBJoin(const NodeID, boost::shared_ptr<Operator>, boost::shared_ptr<Scan>,
           const Query *);
    NBJoin();
    ~NBJoin();

    RC Open(const char * = NULL);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

protected:
    enum { RIGHT_OPEN, RIGHT_RESCAN, RIGHT_GETNEXT, RIGHT_SWEEPBUFFER } state;
    bool leftDone;
    std::vector<Tuple> leftTuples;
    std::vector<Tuple>::const_iterator leftTuplesIt;
    Tuple rightTuple;
    boost::shared_array<char> mainBuffer;
    boost::shared_array<char> overflowBuffer;

private:
    NBJoin(const NBJoin &);
    NBJoin& operator=(const NBJoin &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Join>(*this);
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(op::NBJoin)

#endif
