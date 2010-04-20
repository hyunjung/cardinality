#ifndef CLIENT_DUMMY_H_
#define CLIENT_DUMMY_H_

#include "client/Operator.h"


namespace cardinality {

class Dummy: public Operator {
public:
    explicit Dummy(const NodeID);
    Dummy();
    ~Dummy();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

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

private:
    Dummy& operator=(const Dummy &);
};

}  // namespace cardinality

#endif  // CLIENT_DUMMY_H_
