#include "client/PartitionStats.h"
#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#define PAGE_SIZE 4096


namespace cardinality {

PartitionStats::PartitionStats(const std::string filename,
                               const int num_input_cols,
                               const ValueType pkey_type)
    : num_pages_(), cardinality_(), col_lengths_(num_input_cols), min_val_(), max_val_()
{
    init(filename, num_input_cols, pkey_type);
}

PartitionStats::PartitionStats()
    : num_pages_(), cardinality_(), col_lengths_(), min_val_(), max_val_()
{
}

PartitionStats::~PartitionStats()
{
}

static inline void extractPrimaryKey(const char *pos,
                                     const std::size_t size,
                                     const int num_input_cols,
                                     const ValueType pkey_type,
                                     Value &v)
{
#ifdef _GNU_SOURCE
    const char *delim = static_cast<const char *>(
                            rawmemchr(pos, (num_input_cols == 0) ? '\n' : '|'));
#else
    const char *delim = static_cast<const char *>(
                            std::memchr(pos, (num_input_cols == 0) ? '\n' : '|', size));
#endif
    v.type = pkey_type;
    if (v.type == INT) {
        v.intVal = cardinality::Operator::parseInt(pos, delim - pos);
    } else {  // STRING
        v.intVal = delim - pos;
        std::memcpy(v.charVal, pos, v.intVal);
        v.charVal[v.intVal] = '\0';
    }
}

void PartitionStats::init(const std::string filename,
                          const int num_input_cols,
                          const ValueType pkey_type)
{
    std::size_t file_size = boost::filesystem::file_size(filename);
    num_pages_ = (file_size + PAGE_SIZE - 1) / PAGE_SIZE;

    // 1024 bytes per column
    std::size_t sample_size = ((num_input_cols + 3) / 4) * PAGE_SIZE;
    boost::iostreams::mapped_file_source file(filename,
                                              std::min(sample_size, file_size));
    const char *pos = file.begin();

    // extract the minimum primary key
    extractPrimaryKey(pos, file.size(), num_input_cols, pkey_type, min_val_);

    // accumulate lengths of columns
    std::size_t num_tuples = 0;
    int i = 0;
    for (; pos < file.end(); ++num_tuples, i = 0) {
        for (; i < num_input_cols; ++i) {
            const char *delim
                = static_cast<const char *>(
                      std::memchr(pos, (i == num_input_cols - 1) ? '\n' : '|', file.end() - pos));
            if (delim == NULL) {
                pos = file.end();
                break;
            }
            col_lengths_[i] += delim - pos + 1;
            pos = delim + 1;
        }
    }

    // map the last part of file
    if (sample_size < file_size) {
        file.close();
        std::size_t offset = (file_size - sample_size)
                        & ~(boost::iostreams::mapped_file_source::alignment() - 1);
        file.open(filename, file_size - offset, offset);
    }

    // extract the maximum primary key
#ifdef _GNU_SOURCE
    pos = static_cast<const char *>(memrchr(file.begin(), '\n', file.size() - 1)) + 1;
#else
    for (pos = file.end() - 2; *pos != '\n'; --pos);
    ++pos;
#endif
    extractPrimaryKey(pos, file.end() - pos, num_input_cols, pkey_type, max_val_);

    file.close();

    // compute average lengths of columns and the cardinality
    double tuple_length = 0;
    for (int j = 0; j < num_input_cols; ++j) {
        col_lengths_[j] /= (j < i) ? (num_tuples + 1) : num_tuples;
        tuple_length += col_lengths_[j];
    }
    cardinality_ = file_size / tuple_length;
}

}  // namespace cardinality
