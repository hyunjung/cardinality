#ifndef OPERATOR_H
#define OPERATOR_H

#include <iostream>
#include <boost/serialization/access.hpp>
#include <boost/serialization/assume_abstract.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include "../include/client.h"


namespace ca {

typedef int RC;
typedef int ColID;
typedef int NodeID;

// pointer and length for each value
typedef std::vector<std::pair<const char *, uint32_t> > Tuple;


class Operator {
public:
    typedef boost::shared_ptr<Operator> Ptr;

    Operator(const NodeID);
    virtual ~Operator();
    virtual Operator::Ptr clone() const = 0;

    virtual RC Open(const char * = NULL, const uint32_t = 0) = 0;
    virtual RC ReOpen(const char * = NULL, const uint32_t = 0) = 0;
    virtual RC GetNext(Tuple &) = 0;
    virtual RC Close() = 0;

    virtual void print(std::ostream &, const int = 0) const = 0;
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual ValueType getColType(const char *) const = 0;

    virtual double estCost() const = 0;
    virtual double estCardinality() const = 0;
    virtual double estTupleLength() const = 0;
    virtual double estColLength(ColID) const = 0;

    NodeID getNodeID() const;
    size_t numOutputCols() const;
    ColID getOutputColID(const char *) const;

    static uint32_t parseInt(const char *, const uint32_t);

protected:
    Operator();
    Operator(const Operator &);

    void initProject(const Query *q);

    const NodeID nodeID;
    std::vector<ColID> selectedInputColIDs;

private:
    Operator& operator=(const Operator &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & const_cast<NodeID &>(nodeID);
        ar & selectedInputColIDs;
    }
};

}

BOOST_SERIALIZATION_ASSUME_ABSTRACT(ca::Operator)
BOOST_SERIALIZATION_SHARED_PTR(ca::Operator)

#endif
