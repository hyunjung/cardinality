#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Union.h"
#include "client/Remote.h"
#include <boost/serialization/export.hpp>
#include <boost/serialization/extended_type_info.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/split_free.hpp>

namespace boost {
namespace serialization {

template<class Archive, class T1, class T2, class T3>
void serialize(Archive &ar, boost::tuple<T1, T2, T3> &t, const unsigned int)
{
    ar & t.get<0>();
    ar & t.get<1>();
    ar & t.get<2>();
}

template<class T1, class T2, class T3>
struct implementation_level<boost::tuple<T1, T2, T3> > {
    typedef mpl::integral_c_tag tag;
    typedef mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(
        int,
        value = implementation_level::type::value
    );
};

template<class T1, class T2, class T3>
struct implementation_level<std::vector<boost::tuple<T1, T2, T3> > > {
    typedef mpl::integral_c_tag tag;
    typedef mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(
        int,
        value = implementation_level::type::value
    );
};

template<class Archive, class T1, class T2, class T3, class T4>
void serialize(Archive &ar, boost::tuple<T1, T2, T3, T4> &t, const unsigned int)
{
    ar & t.get<0>();
    ar & t.get<1>();
    ar & t.get<2>();
    ar & t.get<3>();
}

template<class T1, class T2, class T3, class T4>
struct implementation_level<boost::tuple<T1, T2, T3, T4> > {
    typedef mpl::integral_c_tag tag;
    typedef mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(
        int,
        value = implementation_level::type::value
    );
};

template<class T1, class T2, class T3, class T4>
struct implementation_level<std::vector<boost::tuple<T1, T2, T3, T4> > > {
    typedef mpl::integral_c_tag tag;
    typedef mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(
        int,
        value = implementation_level::type::value
    );
};

}}

namespace boost {
namespace serialization {

template<class Archive>
void save(Archive &ar, const boost::asio::ip::address_v4 &a, const unsigned int)
{
    unsigned long addr = a.to_ulong();
    ar << addr;
}

template<class Archive>
void load(Archive &ar, boost::asio::ip::address_v4 &a, const unsigned int)
{
    unsigned long addr;
    ar >> addr;
    a = boost::asio::ip::address_v4(addr);
}

}}

BOOST_SERIALIZATION_SPLIT_FREE(boost::asio::ip::address_v4)

BOOST_CLASS_IMPLEMENTATION(boost::asio::ip::address_v4,
                           boost::serialization::object_serializable)


BOOST_CLASS_EXPORT_GUID(cardinality::SeqScan, "Q")
BOOST_CLASS_EXPORT_GUID(cardinality::IndexScan, "I")
BOOST_CLASS_EXPORT_GUID(cardinality::NLJoin, "L")
BOOST_CLASS_EXPORT_GUID(cardinality::NBJoin, "B")
BOOST_CLASS_EXPORT_GUID(cardinality::Union, "U")
BOOST_CLASS_EXPORT_GUID(cardinality::Remote, "R")
