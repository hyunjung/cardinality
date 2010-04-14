#ifndef CLIENT_JOIN_H_
#define CLIENT_JOIN_H_

#include <boost/tuple/tuple.hpp>
#include "client/Project.h"


namespace cardinality {

class Join: public Project {
public:
    Join(const NodeID, Operator::Ptr, Operator::Ptr,
         const Query *, const int = -1);
    Join();
    Join(const Join &);
    virtual ~Join();

    void Serialize(google::protobuf::io::CodedOutputStream *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    const PartitionStats *getPartitionStats(const char *) const;
    ValueType getColType(const char *) const;

    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    void initFilter(const Query *q, const int);
    bool execFilter(const Tuple &, const Tuple &) const;
    void execProject(const Tuple &, const Tuple &, Tuple &) const;

    Operator::Ptr left_child_;
    Operator::Ptr right_child_;
    std::vector<boost::tuple<ColID, ColID, bool, bool> > join_conds_;

private:
    Join& operator=(const Join &);
};

}  // namespace cardinality

#endif  // CLIENT_JOIN_H_
