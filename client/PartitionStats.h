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
    Value min_val_;
    Value max_val_;

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
        ar & min_val_;
        ar & max_val_;
    }
};

}  // namespace cardinality

#endif  // CLIENT_PARTITIONSTATS_H_
