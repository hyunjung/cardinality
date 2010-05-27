#ifndef CLIENT_MATERIAL_H_
#define CLIENT_MATERIAL_H_

#include <vector>
#include <cstring>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/shared_array.hpp>
#include "client/Operator.h"


namespace cardinality {

class Material: public Operator {
public:
    Material(const NodeID, Operator::Ptr);
    Material(const Material &);
    ~Material();
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
    Operator::Ptr child_;
    boost::shared_ptr<std::vector<Tuple> > tuples_;
    std::vector<Tuple>::const_iterator tuples_it_;
    boost::shared_array<char> buffer_;

private:
    Material& operator=(const Material &);
};

}  // namespace cardinality

#endif  // CLIENT_MATERIAL_H_
