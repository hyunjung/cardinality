#include <cstring>
#include <boost/archive/text_iarchive.hpp>
#include "../include/client.h"
#include "Client.h"
#include "SeqScan.h"
#include "NLJoin.h"
#include "Remote.h"


static const Nodes *sNodes;
static const Data *sData;
static std::map<std::string, Table * > sTables;

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes, const Data *data, const Queries *preset)
{
    sNodes = nodes;
    sData = data;

    for (int i = 0; i < data->nbTables; ++i) {
        sTables[std::string(data->tables[i].tableName)] = &data->tables[i];
    }
}

void startSlave(const Node *masterNode, const Node *currentNode)
{
    using boost::asio::ip::tcp;
    boost::asio::io_service io_service;
    tcp::acceptor acceptor(io_service, tcp::endpoint(tcp::v4(), 30000 + currentNode->iNode));

    op::Tuple tuple;

    while (true) {
        tcp::iostream tcpstream;
        acceptor.accept(*tcpstream.rdbuf());
        boost::archive::text_iarchive ia(tcpstream);
        ia.register_type(static_cast<op::NLJoin *>(NULL));
        ia.register_type(static_cast<op::SeqScan *>(NULL));
        ia.register_type(static_cast<op::Remote *>(NULL));

        boost::shared_ptr<op::Operator> root;
        ia >> root;

        root->Open();
        while (!root->GetNext(tuple)) {
            for (size_t i = 0; i < tuple.size(); i++) {
                if (i == tuple.size() - 1) {
                    tcpstream << tuple[i] << std::endl;
                } else {
                    tcpstream << tuple[i] << "|";
                }
            }
        }
        root->Close();
    }
}

Connection *createConnection()
{
    return new Connection();
}

void closeConnection(Connection *conn)
{
    delete conn;
}

void performQuery(Connection *conn, const Query *q)
{
    conn->q = q;

    boost::shared_ptr<op::Scan> right;

    // construct an execution tree
    for (int i = 0; i < q->nbTable; ++i) {
        std::string tableName(q->tableNames[i]);
        Table *table = sTables[tableName];
        Partition *part = &table->partitions[0];
        right = boost::shared_ptr<op::Scan>(new op::SeqScan(part->iNode, part->fileName, q->aliasNames[i], table, q));
        if (i == 0) {
            conn->root = right;
        } else {
            if (conn->root->getNodeID() == right->getNodeID()) {
                conn->root = boost::shared_ptr<op::Operator>(new op::NLJoin(right->getNodeID(), conn->root, right, q));
            } else {
                conn->root = boost::shared_ptr<op::Operator>(new op::Remote(right->getNodeID(), conn->root, sNodes->nodes[right->getNodeID()].ip));
                conn->root = boost::shared_ptr<op::Operator>(new op::NLJoin(right->getNodeID(), conn->root, right, q));
            }
        }
    }
    if (conn->root->getNodeID() != 0) {
        conn->root = boost::shared_ptr<op::Operator>(new op::Remote(0, conn->root, sNodes->nodes[conn->root->getNodeID()].ip));
    }

    conn->root->print(std::cout);
    conn->root->Open();
}

ErrCode fetchRow(Connection *conn, Value *values)
{
    if (conn->root->GetNext(conn->tuple)) {
        conn->root->Close();
        return DB_END;
    }

    for (int i = 0; i < conn->q->nbOutputFields; ++i) {
        op::ColID cid = conn->root->getOutputColID(conn->q->outputFields[i]);
        values[i].type = conn->root->getColType(conn->q->outputFields[i]);
        if (values[i].type == INT) {
            values[i].intVal = static_cast<uint32_t>(atoi(conn->tuple[cid]));
//          std::cout << values[i].intVal << "|";
        } else {
            strcpy(values[i].charVal, conn->tuple[cid]);
//          std::cout << values[i].charVal << "|";
        }
    }
//  std::cout << std::endl;

    return SUCCESS;
}

void closeProcess()
{
}
