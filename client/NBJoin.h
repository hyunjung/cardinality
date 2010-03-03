#ifndef CLIENT_NBJOIN_H_
#define CLIENT_NBJOIN_H_

#include <boost/scoped_array.hpp>
#include "Join.h"


namespace ca {

class NBJoin: public Join {
public:
    typedef boost::shared_ptr<NBJoin> Ptr;

    NBJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *);
    ~NBJoin();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

    double estCost() const;

protected:
    NBJoin();
    NBJoin(const NBJoin &);

    enum { RIGHT_OPEN, RIGHT_REOPEN, RIGHT_GETNEXT, RIGHT_SWEEPBUFFER } state_;
    bool left_done_;
    std::vector<Tuple> left_tuples_;
    std::vector<Tuple>::const_iterator left_tuples_it_;
    Tuple right_tuple_;
    boost::scoped_array<char> main_buffer_;
    boost::scoped_array<char> overflow_buffer_;

private:
    NBJoin& operator=(const NBJoin &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Join>(*this);
    }
};

}  // namespace ca

BOOST_SERIALIZATION_SHARED_PTR(ca::NBJoin)

#endif  // CLIENT_NBJOIN_H_
