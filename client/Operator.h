#ifndef CLIENT_OPERATOR_H_
#define CLIENT_OPERATOR_H_

#include <iostream>
#include <boost/serialization/access.hpp>
#include <boost/serialization/assume_abstract.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include "include/client.h"

#define COST_DISK_READ_PAGE 1.0
#define COST_DISK_SEEK_PAGE 0.5
#define COST_NET_XFER_BYTE 0.0025
#define SELECTIVITY_EQ 0.1
#define SELECTIVITY_GT 0.4


namespace cardinality {

typedef int ColID;
typedef int NodeID;

// pointer and length for each value
typedef std::vector<std::pair<const char *, uint32_t> > Tuple;


class Operator {
public:
    typedef boost::shared_ptr<Operator> Ptr;

    explicit Operator(const NodeID);
    virtual ~Operator();
    virtual Operator::Ptr clone() const = 0;

    virtual void Open(const char * = NULL, const uint32_t = 0) = 0;
    virtual void ReOpen(const char * = NULL, const uint32_t = 0) = 0;
    virtual bool GetNext(Tuple &) = 0;
    virtual void Close() = 0;

    virtual void print(std::ostream &, const int = 0) const = 0;
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual ValueType getColType(const char *) const = 0;

    virtual double estCost() const = 0;
    virtual double estCardinality() const = 0;
    virtual double estTupleLength() const = 0;
    virtual double estColLength(ColID) const = 0;

    NodeID node_id() const;
    size_t numOutputCols() const;
    ColID getOutputColID(const char *) const;

    static uint32_t parseInt(const char *, const uint32_t);

protected:
    Operator();
    Operator(const Operator &);

    void initProject(const Query *q);

    const NodeID node_id_;
    std::vector<ColID> selected_input_col_ids_;

private:
    Operator& operator=(const Operator &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & const_cast<NodeID &>(node_id_);
        ar & selected_input_col_ids_;
    }
};

}  // namespace cardinality

BOOST_SERIALIZATION_ASSUME_ABSTRACT(cardinality::Operator)
BOOST_SERIALIZATION_SHARED_PTR(cardinality::Operator)

#endif  // CLIENT_OPERATOR_H_
