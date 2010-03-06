#ifndef CLIENT_UNION_H_
#define CLIENT_UNION_H_

#include "client/Operator.h"


namespace cardinality {

class Union: public Operator {
public:
    typedef boost::shared_ptr<Union> Ptr;

    Union(const NodeID, std::vector<Operator::Ptr>);
    ~Union();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

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

}  // namespace cardinality

BOOST_SERIALIZATION_SHARED_PTR(cardinality::Union)

#endif  // CLIENT_UNION_H_
