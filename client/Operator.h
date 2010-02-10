#ifndef OPERATOR_H
#define OPERATOR_H

#include <iostream>
#include <boost/serialization/access.hpp>
#include <boost/serialization/assume_abstract.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include "../include/client.h"


namespace op {

typedef int RC;
typedef unsigned int ColID;
typedef unsigned int NodeID;
typedef std::vector<const char *> Tuple;


class Operator {
public:
    Operator(const NodeID);
    Operator();
    virtual ~Operator();

    virtual RC Open(const char * = NULL) = 0;
    virtual RC GetNext(Tuple &) = 0;
    virtual RC Close() = 0;

    virtual void print(std::ostream &, const int = 0) const = 0;
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual ValueType getColType(const char *) const = 0;

    NodeID getNodeID() const;
    size_t numOutputCols() const;
    ColID getOutputColID(const char *) const;

protected:
    void initProject(const Query *q);
    void printOutputCols(std::ostream &) const;

    const NodeID nodeID;
    std::vector<ColID> selectedInputColIDs;

private:
    Operator(const Operator &);
    Operator& operator=(const Operator &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & const_cast<NodeID &>(nodeID);
        ar & selectedInputColIDs;
    }
};

}

BOOST_SERIALIZATION_ASSUME_ABSTRACT(op::Operator)
BOOST_SERIALIZATION_SHARED_PTR(op::Operator)

#endif
