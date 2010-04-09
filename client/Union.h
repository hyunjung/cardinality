#ifndef CLIENT_UNION_H_
#define CLIENT_UNION_H_

#include "client/Operator.h"


namespace cardinality {

class Union: public Operator {
public:
    Union(const NodeID, std::vector<Operator::Ptr>);
    Union(const Union &);
    ~Union();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    const PartitionStats *getPartitionStats(const char *) const;
    ValueType getColType(const char *) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

    double estCost(const double = 0.0) const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Union();

    std::vector<Operator::Ptr> children_;

    std::size_t it_;
    std::vector<bool> done_;

private:
    Union& operator=(const Union &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & children_;
    }
};

}  // namespace cardinality

BOOST_CLASS_IMPLEMENTATION(cardinality::Union,
                           boost::serialization::object_serializable)
BOOST_CLASS_TRACKING(cardinality::Union,
                     boost::serialization::track_never)

#endif  // CLIENT_UNION_H_
