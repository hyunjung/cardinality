#include "client/Server.h"
#include <boost/thread/thread.hpp>
#include <boost/bind/bind.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"


namespace cardinality {

Server::Server(const int port)
    : io_service_(),
      acceptor_(io_service_, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      new_socket_(new boost::asio::ip::tcp::socket(io_service_))
{
    acceptor_.async_accept(*new_socket_,
                           boost::bind(&Server::handle_accept, this,
                                       boost::asio::placeholders::error));
}

Server::~Server()
{
}

void Server::run()
{
    io_service_.run();
}

void Server::stop()
{
    io_service_.stop();
}

boost::asio::io_service & Server::io_service()
{
    return io_service_;
}

static void handle_query(socket_ptr &sock)
{
    char header[4];
    boost::asio::read(*sock, boost::asio::buffer(header));

    std::istringstream is(std::string(header, sizeof(header)));
    std::size_t body_size;
    is >> std::hex >> body_size;

    boost::asio::streambuf body;
    boost::asio::read(*sock, body.prepare(body_size));
    body.commit(body_size);

    std::istream body_stream(&body);
    boost::archive::binary_iarchive ia(body_stream);
    ia.register_type(static_cast<NLJoin *>(NULL));
    ia.register_type(static_cast<NBJoin *>(NULL));
    ia.register_type(static_cast<SeqScan *>(NULL));
    ia.register_type(static_cast<IndexScan *>(NULL));
    ia.register_type(static_cast<Remote *>(NULL));
    ia.register_type(static_cast<Union *>(NULL));

    Tuple tuple;
    Operator::Ptr root;
    ia >> root;

    std::vector<boost::asio::const_buffer> buffers;
    char delim[2] = {'|', '\n'};

    root->Open();
    while (!root->GetNext(tuple)) {
        buffers.clear();
        for (std::size_t i = 0; i < tuple.size(); i++) {
            buffers.push_back(boost::asio::buffer(tuple[i].first, tuple[i].second));
            if (i < tuple.size() - 1) {
                buffers.push_back(boost::asio::buffer(&delim[0], 1));
            }
        }
        buffers.push_back(boost::asio::buffer(&delim[1], 1));
        boost::asio::write(*sock, buffers);
    }
    root->Close();
}

static void handle_param_query(socket_ptr &sock)
{
    char header[4];
    boost::asio::read(*sock, boost::asio::buffer(header));

    std::istringstream is(std::string(header, sizeof(header)));
    std::size_t body_size;
    is >> std::hex >> body_size;

    boost::asio::streambuf body;
    boost::asio::read(*sock, body.prepare(body_size));
    body.commit(body_size);

    std::istream body_stream(&body);
    boost::archive::binary_iarchive ia(body_stream);
    ia.register_type(static_cast<IndexScan *>(NULL));

    Tuple tuple;
    Operator::Ptr root;
    ia >> root;

    std::vector<boost::asio::const_buffer> buffers;
    char delim[2] = {'|', '\n'};

    boost::asio::streambuf buf;
    boost::system::error_code error;
    std::size_t bytes_read;

    bool reopen = false;

    for (;;) {
        bytes_read = boost::asio::read_until(*sock, buf, '\n', error);
        if (error) {
            if (error == boost::asio::error::eof) {
                break;
            }
            throw boost::system::system_error(error);
        }
        int left_len = std::atoi(boost::asio::buffer_cast<const char *>(buf.data()));
        buf.consume(bytes_read);

        boost::asio::read(*sock, buf.prepare(left_len));
        const char *left_ptr = boost::asio::buffer_cast<const char *>(buf.data());

        if (reopen) {
            root->ReOpen(left_ptr, left_len);
        } else {
            root->Open(left_ptr, left_len);
            reopen = true;
        }

        while (!root->GetNext(tuple)) {
            buffers.clear();
            for (std::size_t i = 0; i < tuple.size(); i++) {
                buffers.push_back(boost::asio::buffer(tuple[i].first, tuple[i].second));
                if (i < tuple.size() - 1) {
                    buffers.push_back(boost::asio::buffer(&delim[0], 1));
                }
            }
            buffers.push_back(boost::asio::buffer(&delim[1], 1));
            boost::asio::write(*sock, buffers);
        }
        // end of results
        boost::asio::write(*sock, boost::asio::buffer(&delim[0], 2));
    }
    root->Close();
}

static void handle_stats(socket_ptr &sock)
{
    boost::asio::streambuf buf;
    boost::system::error_code error;
    std::size_t bytes_read;

    for (;;) {
        bytes_read = boost::asio::read_until(*sock, buf, '\n', error);
        if (error) {
            if (error == boost::asio::error::eof) {
                break;
            }
            throw boost::system::system_error(error);
        }

        char *data = const_cast<char *>(boost::asio::buffer_cast<const char *>(buf.data()));
        buf.consume(bytes_read);

        std::istringstream is(std::string(data, 3));
        int nbFields;
        is >> std::hex >> nbFields;
        enum ValueType fieldType = (data[3] == '0') ? INT : STRING;
        data[bytes_read - 1] = '\0';  // remove the trailing '\n'

        PartitionStats *stats = new PartitionStats(data + 4, nbFields, fieldType);

        std::ostream body_stream(&buf);
        boost::archive::binary_oarchive oa(body_stream);
        oa.register_type(static_cast<PartitionStats *>(NULL));
        oa << stats;

        delete stats;

        boost::asio::write(*sock, buf);
    }
}

static void handle_request(socket_ptr sock)
{
    char command;
    boost::asio::read(*sock, boost::asio::buffer(&command, 1));

    switch (command) {
    case 'Q':
        handle_query(sock);
        break;

    case 'P':
        handle_param_query(sock);
        break;

    case 'S':
        handle_stats(sock);
        break;

    default:
        throw std::runtime_error("unknown command");
    }

    sock->close();
}

void Server::handle_accept(const boost::system::error_code &e)
{
    if (!e) {
        boost::thread t(boost::bind(handle_request, new_socket_));
        new_socket_.reset(new boost::asio::ip::tcp::socket(io_service_));
        acceptor_.async_accept(*new_socket_,
                               boost::bind(&Server::handle_accept, this,
                                           boost::asio::placeholders::error));
    } else {
        throw boost::system::system_error(e);
    }
}

}  // namespace cardinality
