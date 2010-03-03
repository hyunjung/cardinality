#ifndef SCAN_H
#define SCAN_H

#include <boost/tuple/tuple.hpp>
#ifndef USE_STD_IFSTREAM_FOR_SCAN
#include <boost/iostreams/device/mapped_file.hpp>
#else
#include <fstream>
#include <cstring>
#include <boost/scoped_array.hpp>
#endif
#include "Operator.h"
#include "PartitionStats.h"
#include "Value.h"
#include "optimizer.h"


namespace ca {

enum CompOp {
    EQ,
    GT
};

class Scan: public Operator {
public:
    typedef boost::shared_ptr<Scan> Ptr;

    Scan(const NodeID, const char *, const char *,
         const Table *, const PartitionStats *, const Query *);
    virtual ~Scan();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Scan();
    Scan(const Scan &);

    void initFilter(const Query *q);
    bool execFilter(const Tuple &) const;
    void execProject(const Tuple &, Tuple &) const;
    const char *splitLine(const char *, const char *, Tuple &) const;

    const std::string fileName;
    std::vector<boost::tuple<ColID, Value *, CompOp> > gteqConds;
    std::vector<boost::tuple<ColID, ColID, ValueType> > joinConds;
    const int numInputCols;

    const std::string alias;
    const Table *table;
    const PartitionStats *stats;
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    boost::iostreams::mapped_file_source file;
#else
    std::ifstream file;
    boost::scoped_array<char> lineBuffer;
#endif

private:
    Scan& operator=(const Scan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & const_cast<std::string &>(fileName);
        ar & gteqConds;
        ar & joinConds;
        ar & const_cast<int &>(numInputCols);
    }
};

}

BOOST_SERIALIZATION_ASSUME_ABSTRACT(ca::Scan)
BOOST_SERIALIZATION_SHARED_PTR(ca::Scan)

namespace boost {
namespace serialization {

template<class Archive, class T1, class T2, class T3>
void serialize(Archive &ar, boost::tuple<T1, T2, T3> &t, const unsigned int ver) {
    ar & t.get<0>();
    ar & t.get<1>();
    ar & t.get<2>();
}

}}

#endif
