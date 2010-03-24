#ifndef CLIENT_JOIN_H_
#define CLIENT_JOIN_H_

#include <boost/tuple/tuple.hpp>
#include "client/Project.h"


namespace cardinality {

class Join: public Project {
public:
    Join(const NodeID, Operator::Ptr, Operator::Ptr,
         const Query *, const int = -1);
    virtual ~Join();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    ValueType getColType(const char *) const;

    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Join();
    Join(const Join &);

    void initFilter(const Query *q, const int);
    bool execFilter(const Tuple &, const Tuple &) const;
    void execProject(const Tuple &, const Tuple &, Tuple &) const;

    Operator::Ptr left_child_;
    Operator::Ptr right_child_;
    std::vector<boost::tuple<ColID, ColID, bool, bool> > join_conds_;

private:
    Join& operator=(const Join &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Project>(*this);
        ar & left_child_;
        ar & right_child_;
        ar & join_conds_;
    }
};

}  // namespace cardinality

BOOST_SERIALIZATION_ASSUME_ABSTRACT(cardinality::Join)
BOOST_SERIALIZATION_SHARED_PTR(cardinality::Join)

namespace boost {
namespace serialization {

template<class Archive, class T1, class T2, class T3, class T4>
void serialize(Archive &ar, boost::tuple<T1, T2, T3, T4> &t, const unsigned int ver) {
    ar & t.get<0>();
    ar & t.get<1>();
    ar & t.get<2>();
    ar & t.get<3>();
}

}}

#endif  // CLIENT_JOIN_H_
