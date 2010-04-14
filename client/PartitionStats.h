#ifndef CLIENT_PARTITIONSTATS_H_
#define CLIENT_PARTITIONSTATS_H_

#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include "include/client.h"


namespace cardinality {

class PartitionStats {
public:
    PartitionStats(const std::string, const int, const ValueType,
                   const int = 0);
    PartitionStats();
    ~PartitionStats();

    void Serialize(google::protobuf::io::CodedOutputStream *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

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
};

}  // namespace cardinality

#endif  // CLIENT_PARTITIONSTATS_H_
