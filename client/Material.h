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

struct HashQuery {
    std::size_t operator()(const Query *q) const {
        return (q->nbTable << 20)
               + (q->nbOutputFields << 12)
               + (q->nbJoins << 8)
               + (q->nbRestrictionsEqual << 4)
               + (q->nbRestrictionsGreaterThan);
    }
};

struct EqualToQuery {
    bool operator()(const Query *a, const Query *b) const {
        if (a->nbTable != b->nbTable
            || a->nbOutputFields != b->nbOutputFields
            || a->nbJoins != b->nbJoins
            || a->nbRestrictionsEqual != b->nbRestrictionsEqual
            || a->nbRestrictionsGreaterThan != b->nbRestrictionsGreaterThan) {
            return false;
        }

        for (int i = 0; i < a->nbTable; ++i) {
            if (std::strcmp(a->tableNames[i], b->tableNames[i])
                || std::strcmp(a->aliasNames[i], b->aliasNames[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbOutputFields; ++i) {
            if (std::strcmp(a->outputFields[i], b->outputFields[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbJoins; ++i) {
            if (std::strcmp(a->joinFields1[i], b->joinFields1[i])
                || std::strcmp(a->joinFields2[i], b->joinFields2[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbRestrictionsEqual; ++i) {
            if (std::strcmp(a->restrictionEqualFields[i],
                            b->restrictionEqualFields[i])) {
                return false;
            }
        }

        for (int i = 0; i < a->nbRestrictionsGreaterThan; ++i) {
            if (std::strcmp(a->restrictionGreaterThanFields[i],
                            b->restrictionGreaterThanFields[i])) {
                return false;
            }
        }

        return true;
    }
};

}  // namespace cardinality

#endif  // CLIENT_MATERIAL_H_
