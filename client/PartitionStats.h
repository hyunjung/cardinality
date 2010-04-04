#ifndef CLIENT_PARTITIONSTATS_H_
#define CLIENT_PARTITIONSTATS_H_

#include <boost/serialization/access.hpp>
#include <boost/serialization/vector.hpp>
#include "client/Value.h"


namespace cardinality {

class PartitionStats {
public:
    PartitionStats(const std::string, const int, const ValueType,
                   const int = 0);
    PartitionStats();
    ~PartitionStats();

    int part_no_;
    std::size_t num_pages_;
    double cardinality_;
    std::vector<double> col_lengths_;
    Value min_pkey_;
    Value max_pkey_;
    const PartitionStats *next_;

private:
    PartitionStats(const PartitionStats &);
    PartitionStats& operator=(const PartitionStats &);

    void init(const std::string, const int, const ValueType);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int) {
        ar & num_pages_;
        ar & cardinality_;
        ar & col_lengths_;
        ar & min_pkey_;
        ar & max_pkey_;
    }
};

}  // namespace cardinality

#endif  // CLIENT_PARTITIONSTATS_H_
