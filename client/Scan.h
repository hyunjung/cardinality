#ifndef CLIENT_SCAN_H_
#define CLIENT_SCAN_H_

#include <boost/tuple/tuple.hpp>
#include "client/Project.h"
#include "client/PartitionStats.h"
#include "client/Value.h"


namespace cardinality {

enum CompOp {
    EQ,
    GT
};

class Scan: public Project {
public:
    Scan(const NodeID, const char *, const char *,
         const Table *, const PartitionStats *, const Query *);
    Scan(const Scan &);
    virtual ~Scan();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    const PartitionStats *getPartitionStats(const char *) const;
    ValueType getColType(const char *) const;

    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Scan();

    void initFilter(const Query *q);
    bool execFilter(const Tuple &) const;
    void execProject(const Tuple &, Tuple &) const;
    const char *parseLine(const char *);

    const std::string filename_;
    std::vector<boost::tuple<Value *, ColID, CompOp> > gteq_conds_;
    std::vector<boost::tuple<ColID, ColID, bool> > join_conds_;
    const int num_input_cols_;

    const std::string alias_;
    const Table *table_;
    const PartitionStats *stats_;
    std::pair<const char *, const char *> file_;
    Tuple input_tuple_;

private:
    Scan& operator=(const Scan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int) {
        ar & boost::serialization::base_object<Project>(*this);
        ar & const_cast<std::string &>(filename_);
        ar & gteq_conds_;
        ar & join_conds_;
        ar & const_cast<int &>(num_input_cols_);
    }
};

}  // namespace cardinality

BOOST_SERIALIZATION_ASSUME_ABSTRACT(cardinality::Scan)

BOOST_CLASS_IMPLEMENTATION(cardinality::Scan,
                           boost::serialization::object_serializable)
BOOST_CLASS_TRACKING(cardinality::Scan,
                     boost::serialization::track_never)

#endif  // CLIENT_SCAN_H_
