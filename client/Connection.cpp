#include "client/Connection.h"
#include <boost/thread/thread.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/streambuf.hpp>
#include <google/protobuf/wire_format_lite_inl.h>
#include "client/Operator.h"
#include "client/PartStats.h"


namespace cardinality {

Connection::Connection(boost::asio::io_service &io_service)
    : socket_(io_service)
{
}

Connection::~Connection()
{
}

void Connection::start()
{
    socket_.set_option(
        boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(1));

    socket_.async_read_some(
        boost::asio::buffer(buffer_),
        boost::bind(&Connection::handle_read,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
}

boost::asio::ip::tcp::socket & Connection::socket()
{
    return socket_;
}

void Connection::handle_read(const boost::system::error_code &e,
                             std::size_t bytes_transferred)
{
    if (e) {
        if (e == boost::asio::error::eof) {
            socket_.close();
            return;
        }
        throw boost::system::system_error(e);
    }

    void (Connection::*handler)();

    switch (buffer_[0]) {
    case 'F':
        handle_query();
        return;

    case 'Q':
        handler = &Connection::handle_query;
        break;

    case 'S':
        handler = &Connection::handle_stats;
        break;

    default:
        throw std::runtime_error("unknown command");
    }

    // launch a new thread
    boost::thread t(handler, shared_from_this());
}

void Connection::handle_query()
{
    using google::protobuf::io::CodedInputStream;

    // receive a request header
    uint8_t header[4];
    uint32_t size;
    boost::asio::read(socket_, boost::asio::buffer(header));
    CodedInputStream::ReadLittleEndian32FromArray(&header[0], &size);

    // receive a request body (serialized query plan)
    std::vector<char> buf(std::max(size, 128u));
    buf.resize(size);
    boost::asio::read(socket_, boost::asio::buffer(buf));

    CodedInputStream cis(reinterpret_cast<const uint8_t *>(buf.data()), size);
    Operator::Ptr root = Operator::parsePlan(&cis);

    // execute the query plan and transfer results
    Tuple tuple;

    root->Open();
    while (!root->GetNext(tuple)) {
        uint32_t tuple_len = 1;
        for (std::size_t i = 0; i < tuple.size(); i++) {
            tuple_len += tuple[i].second;
            if (i < tuple.size() - 1) {
                ++tuple_len;
            }
        }

        buf.resize(tuple_len);
        char *pos = buf.data();
        for (std::size_t i = 0; i < tuple.size(); i++) {
            std::memcpy(pos, tuple[i].first, tuple[i].second);
            pos += tuple[i].second;
            if (i < tuple.size() - 1) {
                *pos++ = '|';
            }
        }
        *pos = '\n';

        boost::asio::write(socket_, boost::asio::buffer(buf));
    }
    root->Close();

    // send a special sequence indicating the end of results
    char delim[2] = {'|', '\n'};
    boost::asio::write(socket_, boost::asio::buffer(&delim[0], 2));

    // flush the send buffer
    socket_.set_option(
        boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(0));

    // thread exits, socket waits for another request
    start();
}

void Connection::handle_stats()
{
    using google::protobuf::io::CodedInputStream;
    using google::protobuf::io::CodedOutputStream;

    boost::asio::streambuf buf;

    for (;;) {
        // receive a request header
        uint8_t header[4];
        uint32_t size;
        boost::asio::read(socket_, boost::asio::buffer(header));
        CodedInputStream::ReadLittleEndian32FromArray(&header[0], &size);
        if (size == 0xffffffff) {
            break;
        }

        // receive a request body
        boost::asio::read(socket_, buf.prepare(size));
        buf.commit(size);

        CodedInputStream cis(
            boost::asio::buffer_cast<const uint8_t *>(buf.data()), size);

        std::string tableName;
        google::protobuf::internal::WireFormatLite::ReadString(
            &cis, &tableName);
        std::string fileName;
        google::protobuf::internal::WireFormatLite::ReadString(
            &cis, &fileName);
        uint32_t fieldType;
        cis.ReadVarint32(&fieldType);
        uint32_t nbFields;
        cis.ReadVarint32(&nbFields);
        std::vector<std::string> fieldNames;
        fieldNames.reserve(nbFields);
        for (int k = 0; k < nbFields; ++k) {
            std::string fieldName;
            google::protobuf::internal::WireFormatLite::ReadString(
                &cis, &fieldName);
            fieldNames.push_back(fieldName);
        }

        buf.consume(size);

        // construct a PartStats object
        PartStats *stats = new PartStats(tableName,
                                         fileName,
                                         static_cast<ValueType>(fieldType),
                                         fieldNames);

        // send a response
        size = stats->ByteSize();

        uint8_t *target = boost::asio::buffer_cast<uint8_t *>(
                              buf.prepare(4 + size));

        target = CodedOutputStream::WriteLittleEndian32ToArray(size, target);
        target = stats->SerializeToArray(target);
        buf.commit(4 + size);

        delete stats;

        boost::asio::write(socket_, buf);
        buf.consume(4 + size);

        // flush the send buffer
        socket_.set_option(
            boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(0));
        socket_.set_option(
            boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(1));
    }

    // thread exits, socket waits for another request
    start();
}

}  // namespace cardinality
