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

BOOST_CLASS_EXPORT_GUID(cardinality::SeqScan, "Q")
BOOST_CLASS_EXPORT_GUID(cardinality::IndexScan, "I")
BOOST_CLASS_EXPORT_GUID(cardinality::NLJoin, "L")
BOOST_CLASS_EXPORT_GUID(cardinality::NBJoin, "B")
BOOST_CLASS_EXPORT_GUID(cardinality::Union, "U")
BOOST_CLASS_EXPORT_GUID(cardinality::Remote, "R")
