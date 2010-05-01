#include "client/PartitionStats.h"
#include <string>
#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/spirit/include/qi.hpp>
#include <google/protobuf/wire_format_lite_inl.h>


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

uint8_t *PartitionStats::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::internal::WireFormatLite;

    target = CodedOutputStream::WriteVarint64ToArray(num_pages_, target);

    target = WireFormatLite::WriteDoubleNoTagToArray(cardinality_, target);

    target = CodedOutputStream::WriteVarint32ToArray(
                 col_lengths_.size(), target);
    for (std::size_t i = 0; i < col_lengths_.size(); ++i) {
        target = WireFormatLite::WriteDoubleNoTagToArray(
                     col_lengths_[i], target);
    }

    target = CodedOutputStream::WriteVarint32ToArray(min_pkey_.type, target);
    target = CodedOutputStream::WriteVarint32ToArray(min_pkey_.intVal, target);
    if (min_pkey_.type == STRING) {
        int len = std::strlen(min_pkey_.charVal);
        target = CodedOutputStream::WriteVarint32ToArray(len, target);
        target = CodedOutputStream::WriteRawToArray(
                     min_pkey_.charVal, len, target);
    }

    target = CodedOutputStream::WriteVarint32ToArray(max_pkey_.type, target);
    target = CodedOutputStream::WriteVarint32ToArray(max_pkey_.intVal, target);
    if (max_pkey_.type == STRING) {
        int len = std::strlen(max_pkey_.charVal);
        target = CodedOutputStream::WriteVarint32ToArray(len, target);
        target = CodedOutputStream::WriteRawToArray(
                     max_pkey_.charVal, len, target);
    }

    return target;
}

int PartitionStats::ByteSize() const
{
    using google::protobuf::internal::WireFormatLite;

    int total_size = 0;

    total_size += WireFormatLite::UInt64Size(num_pages_);

    total_size += 8;

    total_size += WireFormatLite::UInt32Size(col_lengths_.size());
    total_size += 8 * col_lengths_.size();

    total_size += 1;
    total_size += WireFormatLite::UInt32Size(min_pkey_.intVal);
    if (min_pkey_.type == STRING) {
        int len = std::strlen(min_pkey_.charVal);
        total_size += WireFormatLite::UInt32Size(len);
        total_size += len;
    }

    total_size += 1;
    total_size += WireFormatLite::UInt32Size(max_pkey_.intVal);
    if (max_pkey_.type == STRING) {
        int len = std::strlen(max_pkey_.charVal);
        total_size += WireFormatLite::UInt32Size(len);
        total_size += len;
    }

    return total_size;
}

void PartitionStats::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    using google::protobuf::internal::WireFormatLite;

    input->ReadVarint64(&num_pages_);

    WireFormatLite::ReadPrimitive<double, WireFormatLite::TYPE_DOUBLE>(
        input, &cardinality_);

    uint32_t size;
    input->ReadVarint32(&size);
    col_lengths_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        double col_length;
        WireFormatLite::ReadPrimitive<double, WireFormatLite::TYPE_DOUBLE>(
            input, &col_length);
        col_lengths_.push_back(col_length);
    }

    uint32_t temp;
    input->ReadVarint32(&temp);
    min_pkey_.type = static_cast<ValueType>(temp);
    input->ReadVarint32(&min_pkey_.intVal);
    if (min_pkey_.type == STRING) {
        input->ReadVarint32(&temp);
        input->ReadRaw(min_pkey_.charVal, temp);
        min_pkey_.charVal[temp] = '\0';
    }

    input->ReadVarint32(&temp);
    max_pkey_.type = static_cast<ValueType>(temp);
    input->ReadVarint32(&max_pkey_.intVal);
    if (max_pkey_.type == STRING) {
        input->ReadVarint32(&temp);
        input->ReadRaw(max_pkey_.charVal, temp);
        max_pkey_.charVal[temp] = '\0';
    }
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
