#ifndef UNION_H
#define UNION_H

#include "Operator.h"


namespace ca {

class Union: public Operator {
public:
    typedef boost::shared_ptr<Union> Ptr;

    Union(const NodeID, std::vector<Operator::Ptr>);
    ~Union();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

    double estCost() const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Union();
    Union(const Union &);

    std::vector<Operator::Ptr> children_;

    size_t it_;
    std::vector<bool> done_;

private:
    Union& operator=(const Union &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & children_;
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::Union)

#endif
