#ifndef CLIENT_SCAN_H_
#define CLIENT_SCAN_H_

#include <boost/tuple/tuple.hpp>
#ifdef DISABLE_MEMORY_MAPPED_IO
#include <fstream>
#include <boost/smart_ptr/scoped_array.hpp>
#endif
#include "client/Project.h"
#include "client/PartitionStats.h"


namespace cardinality {

enum CompOp {
    EQ,
    GT
};

class Scan: public Project {
public:
    Scan(const NodeID, const char *, const char *,
         const Table *, const PartitionStats *, const Query *);
    Scan();
    Scan(const Scan &);
    virtual ~Scan();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    const PartitionStats *getPartitionStats(const char *) const;
    ValueType getColType(const char *) const;

    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    void initFilter(const Query *q);
    bool execFilter(const Tuple &) const;
    void execProject(const Tuple &, Tuple &) const;
    const char *parseLine(const char *);

    std::string filename_;
    std::vector<boost::tuple<Value *, ColID, CompOp> > gteq_conds_;
    std::vector<boost::tuple<ColID, ColID, bool> > join_conds_;
    uint32_t num_input_cols_;

    const std::string alias_;
    const Table *table_;
    const PartitionStats *stats_;
#ifdef DISABLE_MEMORY_MAPPED_IO
    std::ifstream file_;
    boost::scoped_array<char> buffer_;
#else
    std::pair<const char *, const char *> file_;
#endif
    Tuple input_tuple_;

private:
    Scan& operator=(const Scan &);
};

}  // namespace cardinality

#endif  // CLIENT_SCAN_H_
