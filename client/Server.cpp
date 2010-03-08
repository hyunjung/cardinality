#include "client/Server.h"
#include <boost/thread/thread.hpp>
#include <boost/bind/bind.hpp>
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
      new_tcpstream_(new boost::asio::ip::tcp::iostream())
{
    acceptor_.async_accept(*new_tcpstream_->rdbuf(),
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

static void handle_query(tcpstream_ptr &conn)
{
    boost::archive::binary_iarchive ia(*conn);
    ia.register_type(static_cast<NLJoin *>(NULL));
    ia.register_type(static_cast<NBJoin *>(NULL));
    ia.register_type(static_cast<SeqScan *>(NULL));
    ia.register_type(static_cast<IndexScan *>(NULL));
    ia.register_type(static_cast<Remote *>(NULL));
    ia.register_type(static_cast<Union *>(NULL));

    Tuple tuple;
    Operator::Ptr root;
    ia >> root;

    root->Open();
    while (!root->GetNext(tuple)) {
        for (std::size_t i = 0; i < tuple.size(); i++) {
            conn->write(tuple[i].first, tuple[i].second);
            if (i < tuple.size() - 1) {
                conn->put('|');
            }
        }
        *conn << std::endl;
    }
    root->Close();
}

static void handle_param_query(tcpstream_ptr &conn)
{
    boost::archive::binary_iarchive ia(*conn);
    ia.register_type(static_cast<IndexScan *>(NULL));

    char buf[16];
    char val[MAX_VARCHAR_LEN + 1];
    bool reopen = false;
    Tuple tuple;
    Operator::Ptr root;
    ia >> root;

    for (;;) {
        conn->getline(buf, 16);
        if (buf[0] == '\0') {
            break;
        }
        int len = std::atoi(buf);
        conn->read(val, len);
        val[len] = '\0';

        if (reopen) {
            root->ReOpen(val, len);
        } else {
            root->Open(val, len);
            reopen = true;
        }

        while (!root->GetNext(tuple)) {
            for (std::size_t i = 0; i < tuple.size(); i++) {
                conn->write(tuple[i].first, tuple[i].second);
                if (i < tuple.size() - 1) {
                    conn->put('|');
                }
            }
            *conn << std::endl;
        }
        // end of results
        conn->put('|');
        *conn << std::endl;
    }
    root->Close();
}

static void handle_stats(tcpstream_ptr &conn)
{
    char buf[1024];

    for (;;) {
        conn->getline(buf, 1024);
        if (buf[0] == '\0') {
            break;
        }
        int nbFields = std::atoi(buf);
        conn->getline(buf, 1024);
        enum ValueType fieldType = static_cast<ValueType>(std::atoi(buf));
        conn->getline(buf, 1024);

        PartitionStats *stats = new PartitionStats(buf, nbFields, fieldType);

        boost::archive::binary_oarchive oa(*conn);
        oa.register_type(static_cast<PartitionStats *>(NULL));
        oa << stats;

        delete stats;
    }
}

static void handle_request(tcpstream_ptr conn)
{
    switch (conn->get()) {
    case 'Q':
        handle_query(conn);
        break;

    case 'P':
        handle_param_query(conn);
        break;

    case 'S':
        handle_stats(conn);
        break;

    default:
        throw std::runtime_error("unknown command");
    }

    conn->close();
}

void Server::handle_accept(const boost::system::error_code &e)
{
    if (!e) {
        boost::thread t(boost::bind(handle_request, new_tcpstream_));
        new_tcpstream_.reset(new boost::asio::ip::tcp::iostream());
        acceptor_.async_accept(*new_tcpstream_->rdbuf(),
                               boost::bind(&Server::handle_accept, this,
                                           boost::asio::placeholders::error));
    }
}

}  // namespace cardinality
