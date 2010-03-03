#ifndef CLIENT_JOIN_H_
#define CLIENT_JOIN_H_

#include "Operator.h"
#include "Scan.h"


namespace ca {

class Join: public Operator {
public:
    typedef boost::shared_ptr<Join> Ptr;

    Join(const NodeID, Operator::Ptr, Operator::Ptr,
         const Query *, const int = -1);
    virtual ~Join();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
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
    std::vector<boost::tuple<ColID, ColID, ValueType> > join_conds_;

private:
    Join& operator=(const Join &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & left_child_;
        ar & right_child_;
        ar & join_conds_;
    }
};

}  // namespace ca

BOOST_SERIALIZATION_ASSUME_ABSTRACT(ca::Join)
BOOST_SERIALIZATION_SHARED_PTR(ca::Join)

#endif  // CLIENT_JOIN_H_
