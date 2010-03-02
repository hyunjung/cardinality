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

    size_t numPages;
    double cardinality;
    std::vector<double> colLengths;
    Value minVal;
    Value maxVal;

protected:
    void init(const std::string, const int, const ValueType);

private:
    PartitionStats(const PartitionStats &);
    PartitionStats& operator=(const PartitionStats &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & numPages;
        ar & cardinality;
        ar & colLengths;
        ar & minVal;
        ar & maxVal;
    }
};

}

#endif
