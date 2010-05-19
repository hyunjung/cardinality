#ifndef CLIENT_PARTITIONSTATS_H_
#define CLIENT_PARTITIONSTATS_H_

#include <vector>
#include <string>
#include <google/protobuf/io/coded_stream.h>
#include "include/client.h"


namespace cardinality {

class PartStats {
public:
    PartStats(const std::string &, const std::string &,
              const ValueType, const std::vector<std::string> &, const int = 0);
    PartStats(const Table *, const int);
    PartStats(google::protobuf::io::CodedInputStream *);
    PartStats();
    ~PartStats();

    uint8_t *SerializeToArray(uint8_t *target) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    int part_no_;
    std::size_t num_pages_;
    std::vector<double> num_distinct_values_;
    std::vector<double> col_lengths_;
    Value min_pkey_;
    Value max_pkey_;
    const PartStats *next_;

private:
    PartStats(const PartStats &);
    PartStats& operator=(const PartStats &);

    void init(const std::string, const int, const ValueType);
    void init2(const std::string, const std::vector<std::string> &);
};

}  // namespace cardinality

#endif  // CLIENT_PARTITIONSTATS_H_
