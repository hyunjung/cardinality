#ifndef PARTITIONSTATS_H
#define PARTITIONSTATS_H

#include "Operator.h"
#include "Value.h"


namespace ca {

class PartitionStats {
public:
    PartitionStats(const std::string, const int, const ValueType);
    PartitionStats();
    ~PartitionStats();

    size_t num_pages_;
    double cardinality_;
    std::vector<double> col_lengths_;
    Value min_val_;
    Value max_val_;

protected:
    void init(const std::string, const int, const ValueType);

private:
    PartitionStats(const PartitionStats &);
    PartitionStats& operator=(const PartitionStats &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & num_pages_;
        ar & cardinality_;
        ar & col_lengths_;
        ar & min_val_;
        ar & max_val_;
    }
};

}

#endif
