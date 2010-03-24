#ifndef CLIENT_DUMMY_H_
#define CLIENT_DUMMY_H_

#include "client/Operator.h"


namespace cardinality {

class Dummy: public Operator {
public:
    explicit Dummy(const NodeID);
    ~Dummy();
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
    Dummy();
    Dummy(const Dummy &);

private:
    Dummy& operator=(const Dummy &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
    }
};

}  // namespace cardinality

BOOST_SERIALIZATION_SHARED_PTR(cardinality::Dummy)

#endif  // CLIENT_DUMMY_H_
