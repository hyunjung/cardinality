#ifndef CLIENT_OPERATOR_H_
#define CLIENT_OPERATOR_H_

#include <vector>
#include <utility>  // std::pair
#include <iostream>  // std::ostream
#include <boost/smart_ptr/make_shared.hpp>
#include <google/protobuf/io/coded_stream.h>
#include "include/client.h"


namespace cardinality {

typedef uint16_t ColID;
typedef uint32_t NodeID;

// pointer and length for each value
typedef std::vector<std::pair<const char *, uint32_t> > Tuple;

class PartStats;

class Operator {
public:
    typedef boost::shared_ptr<Operator> Ptr;

    explicit Operator(const NodeID);
    explicit Operator(google::protobuf::io::CodedInputStream *);
    Operator(const Operator &);
    virtual ~Operator();
    virtual Operator::Ptr clone() const = 0;

    virtual void Open(const char * = NULL, const uint32_t = 0) = 0;
    virtual void ReOpen(const char * = NULL, const uint32_t = 0) = 0;
    virtual bool GetNext(Tuple &) = 0;
    virtual void Close() = 0;

    virtual uint8_t *SerializeToArray(uint8_t *) const;
    virtual int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    virtual void print(std::ostream &, const int = 0, const double = 0.0) const = 0;
#endif
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual std::pair<const PartStats *, ColID> getPartStats(const ColID) const = 0;
    virtual ValueType getColType(const char *) const = 0;
    virtual ColID numOutputCols() const = 0;
    virtual ColID getOutputColID(const char *) const = 0;

    virtual double estCost(const double = 0.0) const = 0;
    virtual double estCardinality(const bool = false) const = 0;
    virtual double estTupleLength() const = 0;
    virtual double estColLength(const ColID) const = 0;

    NodeID node_id() const;

    static uint32_t parseInt(const char *, const uint32_t);
    static Operator::Ptr parsePlan(google::protobuf::io::CodedInputStream *);

protected:
    enum { TAG_SEQSCAN, TAG_INDEXSCAN, TAG_NLJOIN, TAG_NBJOIN, TAG_REMOTE, TAG_UNION };

    NodeID node_id_;

private:
    Operator& operator=(const Operator &);
};

}  // namespace cardinality

#endif  // CLIENT_OPERATOR_H_
