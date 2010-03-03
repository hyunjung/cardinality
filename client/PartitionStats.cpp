#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include "PartitionStats.h"

#define PAGE_SIZE 4096

using namespace ca;


PartitionStats::PartitionStats(const std::string fileName,
                               const int numInputCols,
                               const ValueType pkeyType)
    : num_pages_(), cardinality_(), col_lengths_(numInputCols), min_val_(), max_val_()
{
    init(fileName, numInputCols, pkeyType);
}

PartitionStats::PartitionStats()
    : num_pages_(), cardinality_(), col_lengths_(), min_val_(), max_val_()
{
}

PartitionStats::~PartitionStats()
{
}

static inline void extractPrimaryKey(const char *pos,
                                     const size_t size,
                                     const int numInputCols,
                                     const ValueType pkeyType,
                                     Value &v)
{
#ifdef _GNU_SOURCE
    const char *delim = static_cast<const char *>(
                            rawmemchr(pos, (numInputCols == 0) ? '\n' : '|'));
#else
    const char *delim = static_cast<const char *>(
                            std::memchr(pos, (numInputCols == 0) ? '\n' : '|', size));
#endif
    v.type = pkeyType;
    if (v.type == INT) {
        v.intVal = ca::Operator::parseInt(pos, delim - pos);
    } else { // STRING
        v.intVal = delim - pos;
        std::memcpy(v.charVal, pos, v.intVal);
        v.charVal[v.intVal] = '\0';
    }
}

void PartitionStats::init(const std::string fileName,
                          const int numInputCols,
                          const ValueType pkeyType)
{
    size_t fileSize = boost::filesystem::file_size(fileName);
    num_pages_ = (fileSize + PAGE_SIZE - 1) / PAGE_SIZE;

    // 1024 bytes per column
    size_t sampleSize = ((numInputCols + 3) / 4) * PAGE_SIZE;
    boost::iostreams::mapped_file_source file(fileName,
                                              std::min(sampleSize, fileSize));
    const char *pos = file.begin();

    // extract the minimum primary key
    extractPrimaryKey(pos, file.size(), numInputCols, pkeyType, min_val_);

    // accumulate lengths of columns
    size_t numTuples = 0;
    int i = 0;
    for (; pos < file.end(); ++numTuples, i = 0) {
        for (; i < numInputCols; ++i) {
            const char *delim = static_cast<const char *>(
                                    std::memchr(pos, (i == numInputCols - 1) ? '\n' : '|', file.end() - pos));
            if (delim == NULL) {
                pos = file.end();
                break;
            }
            col_lengths_[i] += delim - pos + 1;
            pos = delim + 1;
        }
    }

    // map the last part of file
    if (sampleSize < fileSize) {
        file.close();
        size_t offset = (fileSize - sampleSize) & ~(boost::iostreams::mapped_file_source::alignment() - 1);
        file.open(fileName, fileSize - offset, offset);
    }

    // extract the maximum primary key
#ifdef _GNU_SOURCE
    pos = static_cast<const char *>(memrchr(file.begin(), '\n', file.size() - 1)) + 1;
#else
    for (pos = file.end() - 2; *pos != '\n'; --pos);
    ++pos;
#endif
    extractPrimaryKey(pos, file.end() - pos, numInputCols, pkeyType, max_val_);

    file.close();

    // compute average lengths of columns and the cardinality
    double tupleLength = 0;
    for (int j = 0; j < numInputCols; ++j) {
        col_lengths_[j] /= (j < i) ? (numTuples + 1) : numTuples;
        tupleLength += col_lengths_[j];
    }
    cardinality_ = fileSize / tupleLength;
}
