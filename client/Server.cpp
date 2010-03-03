#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "Server.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "Remote.h"
#include "Union.h"

using namespace ca;


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

static void handle_request(tcpstream_ptr conn)
{
    if (conn->get() == 'S') {
        char buf[1024];

        while (true) {
            conn->getline(buf, 1024);
            if (buf[0] == '\0') {
                break;
            }
            int nbFields = std::atoi(buf);
            conn->getline(buf, 1024);
            enum ValueType fieldType = static_cast<ValueType>(std::atoi(buf));
            conn->getline(buf, 1024);

            boost::archive::binary_oarchive oa(*conn);
            oa.register_type(static_cast<PartitionStats *>(NULL));

            PartitionStats *stats = new PartitionStats(buf, nbFields, fieldType);
            oa << stats;
            delete stats;
        }

    } else { // 'Q'
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
            for (size_t i = 0; i < tuple.size(); i++) {
                conn->write(tuple[i].first, tuple[i].second);
                if (i < tuple.size() - 1) {
                    conn->put('|');
                }
            }
            *conn << std::endl;
        }
        root->Close();
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
