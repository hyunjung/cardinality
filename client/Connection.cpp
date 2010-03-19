#include "client/Connection.h"
#include <boost/thread/thread.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"


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
    case 'Q':
        handler = &Connection::handle_query;
        break;

    case 'P':
        handler = &Connection::handle_param_query;
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
    char header[4];
    boost::asio::read(socket_, boost::asio::buffer(header));

    std::istringstream is(std::string(header, sizeof(header)));
    std::size_t body_size;
    is >> std::hex >> body_size;

    // receive a serialized query plan
    boost::asio::streambuf body;
    boost::asio::read(socket_, body.prepare(body_size));
    body.commit(body_size);

    // deserialize
    std::istream body_stream(&body);
    boost::archive::binary_iarchive ia(body_stream);
    ia.register_type(static_cast<NLJoin *>(NULL));
    ia.register_type(static_cast<NBJoin *>(NULL));
    ia.register_type(static_cast<SeqScan *>(NULL));
    ia.register_type(static_cast<IndexScan *>(NULL));
    ia.register_type(static_cast<Remote *>(NULL));
    ia.register_type(static_cast<Union *>(NULL));

    Operator::Ptr root;
    ia >> root;

    Tuple tuple;
    std::vector<boost::asio::const_buffer> buffers;
    char delim[2] = {'|', '\n'};

    root->Open();
    while (!root->GetNext(tuple)) {
        buffers.clear();
        for (std::size_t i = 0; i < tuple.size(); i++) {
            buffers.push_back(boost::asio::buffer(tuple[i].first,
                                                  tuple[i].second));
            if (i < tuple.size() - 1) {
                buffers.push_back(boost::asio::buffer(&delim[0], 1));
            }
        }
        buffers.push_back(boost::asio::buffer(&delim[1], 1));
        boost::asio::write(socket_, buffers);
    }
    root->Close();

    // send a special sequence indicating the end of results
    boost::asio::write(socket_, boost::asio::buffer(&delim[0], 2));

    // flush the sending buffer
    socket_.set_option(
        boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(0));

    // thread exits, socket waits for another request
    start();
}

void Connection::handle_param_query()
{
    char header[4];
    boost::asio::read(socket_, boost::asio::buffer(header));

    std::istringstream is(std::string(header, sizeof(header)));
    std::size_t body_size;
    is >> std::hex >> body_size;

    // receive a serialized query plan
    boost::asio::streambuf body;
    boost::asio::read(socket_, body.prepare(body_size));
    body.commit(body_size);

    // deserialize
    std::istream body_stream(&body);
    boost::archive::binary_iarchive ia(body_stream);
    ia.register_type(static_cast<IndexScan *>(NULL));

    Operator::Ptr root;
    ia >> root;

    Tuple tuple;
    std::vector<boost::asio::const_buffer> buffers;
    char delim[2] = {'|', '\n'};

    boost::system::error_code error;
    bool reopen = false;

    for (;;) {
        // receive parameters (a key for index lookup)
        boost::asio::read(socket_, boost::asio::buffer(header),
                          boost::asio::transfer_all(), error);
        if (error) {
            if (error == boost::asio::error::eof) {
                break;
            }
            throw boost::system::system_error(error);
        }

        std::istringstream is(std::string(header, sizeof(header)));
        uint32_t left_len;
        is >> std::hex >> left_len;

        boost::asio::read(socket_, body.prepare(left_len));
        const char *left_ptr
            = boost::asio::buffer_cast<const char *>(body.data());
        body.consume(left_len);

        if (reopen) {
            root->ReOpen(left_ptr, left_len);
        } else {
            root->Open(left_ptr, left_len);
            reopen = true;
        }

        while (!root->GetNext(tuple)) {
            buffers.clear();
            for (std::size_t i = 0; i < tuple.size(); i++) {
                buffers.push_back(boost::asio::buffer(tuple[i].first,
                                                      tuple[i].second));
                if (i < tuple.size() - 1) {
                    buffers.push_back(boost::asio::buffer(&delim[0], 1));
                }
            }
            buffers.push_back(boost::asio::buffer(&delim[1], 1));
            boost::asio::write(socket_, buffers);
        }

        // send a special sequence indicating the end of results
        boost::asio::write(socket_, boost::asio::buffer(&delim[0], 2));

        // flush the sending buffer
        socket_.set_option(
            boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(0));
        socket_.set_option(
            boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_CORK>(1));
    }
    root->Close();

    socket_.close();
}

void Connection::handle_stats()
{
    boost::asio::streambuf buf;
    boost::system::error_code error;
    std::size_t bytes_read;

    for (;;) {
        bytes_read = boost::asio::read_until(socket_, buf, '\n', error);
        if (error) {
            if (error == boost::asio::error::eof) {
                break;
            }
            throw boost::system::system_error(error);
        }

        char *data = const_cast<char *>(
                         boost::asio::buffer_cast<const char *>(buf.data()));
        buf.consume(bytes_read);

        std::istringstream is(std::string(data, 3));
        int nbFields;
        is >> std::hex >> nbFields;
        enum ValueType fieldType = (data[3] == '0') ? INT : STRING;
        data[bytes_read - 1] = '\0';  // remove the trailing '\n'

        PartitionStats *stats
            = new PartitionStats(data + 4, nbFields, fieldType);

        std::ostream body_stream(&buf);
        boost::archive::binary_oarchive oa(body_stream);
        oa.register_type(static_cast<PartitionStats *>(NULL));
        oa << stats;

        delete stats;

        boost::asio::write(socket_, buf);
    }

    socket_.close();
}

}  // namespace cardinality
