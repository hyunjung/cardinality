#ifndef CLIENT_PARTITIONSTATS_H_
#define CLIENT_PARTITIONSTATS_H_

#include "client/Operator.h"
#include "client/Value.h"


namespace cardinality {

class PartitionStats {
public:
    PartitionStats(const int, const std::string, const int, const ValueType);
    PartitionStats();
    ~PartitionStats();

    const int part_no_;
    std::size_t num_pages_;
    double cardinality_;
    std::vector<double> col_lengths_;
    Value min_pkey_;
#ifdef PARTITIONSTATS_MAX_PKEY
    Value max_pkey_;
#endif

private:
    PartitionStats(const PartitionStats &);
    PartitionStats& operator=(const PartitionStats &);

    void init(const std::string, const int, const ValueType);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & const_cast<int &>(part_no_);
        ar & num_pages_;
        ar & cardinality_;
        ar & col_lengths_;
        ar & min_pkey_;
#ifdef PARTITIONSTATS_MAX_PKEY
        ar & max_pkey_;
#endif
    }
};

}  // namespace cardinality

#endif  // CLIENT_PARTITIONSTATS_H_
