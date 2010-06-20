// Copyright (c) 2010, Hyunjung Park
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Stanford University nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "client/PartStats.h"
#include <string>
#include <cstring>
#include <algorithm>  // std::min
#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/spirit/include/qi.hpp>
#include <google/protobuf/wire_format_lite_inl.h>
#include "lib/index/include/server.h"


namespace cardinality {

static const int PAGE_SIZE = 4096;

PartStats::PartStats(const Table *table, const int part_no)
    : part_no_(part_no),
      num_pages_(),
      num_distinct_values_(table->nbFields, 20.0),  // Scan::SELECTIVITY_EQ^-1
      col_lengths_(table->nbFields),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
    init(table->partitions[part_no].fileName,
         table->nbFields,
         table->fieldsType[0]);
#ifdef ENABLE_NUM_DISTINCT_VALUES
    std::vector<std::string> fieldnames;
    fieldnames.reserve(table->nbFields);
    for (int i = 0; i < table->nbFields; ++i) {
        fieldnames.push_back(table->fieldsName[i]);
    }
    init2(table->tableName, fieldnames);
#endif
}

PartStats::PartStats(const std::string &tablename,
                     const std::string &filename,
                     const ValueType pkey_type,
                     const std::vector<std::string> &fieldnames)
    : part_no_(0),
      num_pages_(),
      num_distinct_values_(fieldnames.size(), 20.0),
      col_lengths_(fieldnames.size()),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
    init(filename, fieldnames.size(), pkey_type);
#ifdef ENABLE_NUM_DISTINCT_VALUES
    init2(tablename, fieldnames);
#endif
}

PartStats::PartStats(google::protobuf::io::CodedInputStream *input)
    : part_no_(),
      num_pages_(),
      num_distinct_values_(),
      col_lengths_(),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
    Deserialize(input);
}

PartStats::PartStats()
    : part_no_(),
      num_pages_(),
      num_distinct_values_(),
      col_lengths_(),
      min_pkey_(),
      max_pkey_(),
      next_(NULL)
{
}

PartStats::~PartStats()
{
}

uint8_t *PartStats::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::internal::WireFormatLite;

    target = CodedOutputStream::WriteVarint64ToArray(num_pages_, target);

    target = CodedOutputStream::WriteVarint32ToArray(
                 col_lengths_.size(), target);
    for (std::size_t i = 0; i < col_lengths_.size(); ++i) {
        target = WireFormatLite::WriteDoubleNoTagToArray(
                     num_distinct_values_[i], target);
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

int PartStats::ByteSize() const
{
    using google::protobuf::internal::WireFormatLite;

    int total_size = 0;

    total_size += WireFormatLite::UInt64Size(num_pages_);

    total_size += WireFormatLite::UInt32Size(col_lengths_.size());
    total_size += 8 * num_distinct_values_.size();
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

void PartStats::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    using google::protobuf::internal::WireFormatLite;

    input->ReadVarint64(&num_pages_);

    uint32_t size;
    input->ReadVarint32(&size);
    num_distinct_values_.reserve(size);
    col_lengths_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        double temp;
        WireFormatLite::ReadPrimitive<double, WireFormatLite::TYPE_DOUBLE>(
            input, &temp);
        num_distinct_values_.push_back(temp);
        WireFormatLite::ReadPrimitive<double, WireFormatLite::TYPE_DOUBLE>(
            input, &temp);
        col_lengths_.push_back(temp);
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

void PartStats::init(const std::string filename,
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
    num_distinct_values_[0] = file_size / tuple_length;
}

void PartStats::init2(const std::string tablename,
                      const std::vector<std::string> &fieldnames)
{
    Index *index;
    TxnState *txn;
    Record record;
    Value value;

    for (std::size_t i = 1; i < fieldnames.size(); ++i) {
        if (fieldnames[i].empty() || fieldnames[i][0] != '_') {
            continue;
        }

        std::string index_col = tablename + "." + fieldnames[i];

        std::size_t num_values = 0;
        std::size_t num_distinct_values = 0;

        openIndex(index_col.c_str(), &index);
        beginTransaction(&txn);

        while (getNext(index, txn, &record) == SUCCESS) {
            ++num_values;
            if (record.val.type == INT) {
                if (record.val.intVal != value.intVal) {
                    value.intVal = record.val.intVal;
                    ++num_distinct_values;
                }
            } else {  // STRING
                if (std::strcmp(record.val.charVal, value.charVal)) {
                    std::strcpy(value.charVal, record.val.charVal);
                    ++num_distinct_values;
                }
            }
        }

        commitTransaction(txn);
        closeIndex(index);

        num_distinct_values_[i] = num_distinct_values;
        num_distinct_values_[0] = num_values;
    }
}

}  // namespace cardinality
