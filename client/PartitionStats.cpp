#include "client/PartitionStats.h"
#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/spirit/include/qi.hpp>


namespace cardinality {

static const int PAGE_SIZE = 4096;

PartitionStats::PartitionStats(const std::string filename,
                               const int num_input_cols,
                               const ValueType pkey_type,
                               const int part_no)
    : part_no_(part_no),
      num_pages_(),
      cardinality_(),
      col_lengths_(num_input_cols),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
    init(filename, num_input_cols, pkey_type);
}

PartitionStats::PartitionStats()
    : part_no_(),
      num_pages_(),
      cardinality_(),
      col_lengths_(),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
}

PartitionStats::~PartitionStats()
{
}

static inline void extractPrimaryKey(const char *pos,
                                     const int num_input_cols,
                                     const ValueType pkey_type,
                                     Value &v)
{
    const char *delim = static_cast<const char *>(
                            rawmemchr(pos, (num_input_cols == 0) ? '\n' : '|'));
    v.type = pkey_type;
    if (v.type == INT) {
        boost::spirit::qi::parse(pos, delim, boost::spirit::uint_, v.intVal);
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
    extractPrimaryKey(pos, num_input_cols, pkey_type, min_pkey_);

    // accumulate lengths of columns
    std::size_t num_tuples = 0;
    int i = 0;
    for (; pos < file.end(); ++num_tuples, i = 0) {
        for (; i < num_input_cols; ++i) {
            const char *delim
                = static_cast<const char *>(
                      std::memchr(pos,
                                  (i == num_input_cols - 1) ? '\n' : '|',
                                  file.end() - pos));
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
        std::size_t offset
            = (file_size - sample_size)
              & ~(boost::iostreams::mapped_file_source::alignment() - 1);
        file.open(filename, file_size - offset, offset);
    }

    // extract the maximum primary key
    pos = 1 + static_cast<const char *>(
                  memrchr(file.begin(), '\n', file.size() - 1));
    extractPrimaryKey(pos, num_input_cols, pkey_type, max_pkey_);

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
