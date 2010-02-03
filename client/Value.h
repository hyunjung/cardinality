#ifndef VALUE_H
#define VALUE_H

#include <boost/serialization/split_free.hpp>
#include "Operator.h"


namespace boost {
namespace serialization {

template<class Archive>
void save(Archive &ar, const Value &v, const unsigned int ver) {
    ar << v.type;
    ar << v.intVal;
    if (v.type == STRING) {
        std::string buf(v.charVal);
        ar << buf;
    }
}

template<class Archive>
void load(Archive &ar, Value &v, const unsigned int ver) {
    ar >> v.type;
    ar >> v.intVal;
    if (v.type == STRING) {
        std::string buf;
        ar >> buf;
        buf.copy(v.charVal, buf.size());
        v.charVal[buf.size()] = '\0';
    }
}

}}

BOOST_SERIALIZATION_SPLIT_FREE(Value)

#endif