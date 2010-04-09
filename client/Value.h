#ifndef CLIENT_VALUE_H_
#define CLIENT_VALUE_H_

#include <string>
#include <boost/serialization/split_free.hpp>
#include <boost/serialization/tracking.hpp>
#include "include/client.h"


namespace boost {
namespace serialization {

template<class Archive>
void save(Archive &ar, const Value &v, const unsigned int)
{
    ar << v.type;
    ar << v.intVal;
    if (v.type == STRING) {
        std::string buf(v.charVal);
        ar << buf;
    }
}

template<class Archive>
void load(Archive &ar, Value &v, const unsigned int)
{
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

BOOST_CLASS_IMPLEMENTATION(Value,
                           boost::serialization::object_serializable)
BOOST_CLASS_TRACKING(Value,
                     boost::serialization::track_never)

#endif  // CLIENT_VALUE_H_
