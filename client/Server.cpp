#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include "Server.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "Remote.h"


Server::Server(const int port)
    : io_service(),
      acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      new_tcpstream(new boost::asio::ip::tcp::iostream())
{
    acceptor.async_accept(*new_tcpstream->rdbuf(),
                          boost::bind(&Server::handle_accept, this,
                                      boost::asio::placeholders::error));
}

Server::~Server()
{
}

void Server::run()
{
    io_service.run();
}

void Server::stop()
{
    io_service.stop();
}

static void handle_query(tcpstream_ptr conn)
{
    boost::archive::binary_iarchive ia(*conn);
    ia.register_type(static_cast<op::NLJoin *>(NULL));
    ia.register_type(static_cast<op::NBJoin *>(NULL));
    ia.register_type(static_cast<op::SeqScan *>(NULL));
    ia.register_type(static_cast<op::IndexScan *>(NULL));
    ia.register_type(static_cast<op::Remote *>(NULL));

    op::Tuple tuple;
    op::Operator::Ptr root;
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

    conn->close();
}

void Server::handle_accept(const boost::system::error_code &e)
{
    if (!e) {
        boost::thread t(boost::bind(handle_query, new_tcpstream));
        new_tcpstream.reset(new boost::asio::ip::tcp::iostream());
        acceptor.async_accept(*new_tcpstream->rdbuf(),
                              boost::bind(&Server::handle_accept, this,
                                          boost::asio::placeholders::error));
    }
}
