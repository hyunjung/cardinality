#ifndef SCAN_H
#define SCAN_H

#include <fstream>
#include <boost/tuple/tuple.hpp>
#include <boost/shared_array.hpp>
#include "Operator.h"


namespace op {

enum CompOp {
    EQ,
    GT
};

class Scan: public Operator {
public:
    Scan(const Query *, const char *, const Table *, const char *);
    Scan();
    virtual ~Scan();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

protected:
    void initFilter(const Query *q);
    bool execFilter(Tuple &) const;
    void execProject(Tuple &, Tuple &) const;

    const std::string alias;
    const Table *table;
    const std::string fileName;
    std::ifstream ifs;
    boost::shared_array<char> lineBuffer;

    std::vector<boost::tuple<ColID, Value *, CompOp> > gteqConds;
    std::vector<boost::tuple<ColID, ColID, ValueType> > joinConds;

private:
    Scan(const Scan &);
    Scan& operator=(const Scan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & const_cast<std::string &>(alias);
        ar & const_cast<std::string &>(fileName);
        ar & gteqConds;
        ar & joinConds;
    }
};

}

BOOST_SERIALIZATION_ASSUME_ABSTRACT(op::Scan);
BOOST_SERIALIZATION_SHARED_PTR(op::Scan);

namespace boost {
namespace serialization {

template<class Archive, class T1, class T2, class T3>
void serialize(Archive &ar, boost::tuple<T1, T2, T3> &t, const unsigned int ver) {
    ar & t.get<0>();
    ar & t.get<1>();
    ar & t.get<2>();
}

template<class Archive>
void serialize(Archive &ar, Value &v, const unsigned int ver) {
    ar & v.type;
    ar & v.intVal;
    ar & v.charVal;
}

}}

#endif
