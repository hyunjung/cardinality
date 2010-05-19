#ifndef CLIENT_UNION_H_
#define CLIENT_UNION_H_

#include <vector>
#include "client/Operator.h"


namespace cardinality {

class Union: public Operator {
public:
    Union(const NodeID, std::vector<Operator::Ptr>);
    explicit Union(google::protobuf::io::CodedInputStream *);
    Union(const Union &);
    ~Union();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    void print(std::ostream &, const int) const;
#endif
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const char *) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

    double estCost(const double = 0.0) const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    std::vector<Operator::Ptr> children_;

    std::size_t it_;
    std::vector<bool> done_;

private:
    Union& operator=(const Union &);
};

}  // namespace cardinality

#endif  // CLIENT_UNION_H_
