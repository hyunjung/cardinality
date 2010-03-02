#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "../include/client.h"
#include "Server.h"
#include "PartitionStats.h"
#include "optimizer.h"


struct Connection {
    const Query *q;
    ca::Operator::Ptr root;
    ca::Tuple tuple;
};

const Nodes *gNodes;
std::map<std::string, Table * > gTables;
std::map<std::pair<std::string, int>, ca::PartitionStats * > gStats;


static void startPreTreatmentSlave(const ca::NodeID n, const Data *data)
{
    boost::asio::ip::tcp::iostream tcpstream;
    for (int attempt = 0; attempt < 5; ++attempt) {
        tcpstream.connect(gNodes->nodes[n].ip, boost::lexical_cast<std::string>(17000 + n));
        if (tcpstream.good()) {
            break;
        }
        tcpstream.clear();
        usleep(100000); // 0.1s
    }
    if (tcpstream.fail()) {
        throw std::runtime_error("tcp::iostream.connect() failed");
    }

    tcpstream.put('S');

    for (int i = 0; i < data->nbTables; ++i) {
        for (int j = 0; j < data->tables[i].nbPartitions; ++j) {
            if (data->tables[i].partitions[j].iNode != n) {
                continue;
            }

            tcpstream << data->tables[i].nbFields << std::endl;
            tcpstream << data->tables[i].fieldsType[0] << std::endl;
            tcpstream << data->tables[i].partitions[j].fileName << std::endl;

            boost::archive::binary_iarchive ia(tcpstream);
            ia.register_type(static_cast<ca::PartitionStats *>(NULL));

            ca::PartitionStats *stats;
            ia >> stats;
            gStats[std::make_pair(std::string(data->tables[i].tableName), j)] = stats;
        }
    }

    tcpstream.close();
}

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes, const Data *data, const Queries *preset)
{
    gNodes = nodes;

    for (int i = 0; i < data->nbTables; ++i) {
        gTables[std::string(data->tables[i].tableName)] = &data->tables[i];
    }

    usleep(50000); // 0.05s

    boost::thread_group threads;
    for (int n = 1; n < gNodes->nbNodes; ++n) {
        threads.create_thread(boost::bind(&startPreTreatmentSlave, n, data));
    }

    for (int i = 0; i < data->nbTables; ++i) {
        for (int j = 0; j < data->tables[i].nbPartitions; ++j) {
            if (data->tables[i].partitions[j].iNode) {
                continue;
            }
            ca::PartitionStats *stats = new ca::PartitionStats(data->tables[i].partitions[j].fileName,
                                                               data->tables[i].nbFields,
                                                               data->tables[i].fieldsType[0]);
            gStats[std::make_pair(std::string(data->tables[i].tableName), j)] = stats;
        }
    }

    // TODO: *ps will be leaked
    ca::Server *ps = new ca::Server(17000);
    boost::thread t(boost::bind(&ca::Server::run, ps));

    threads.join_all();
}

void startSlave(const Node *masterNode, const Node *currentNode)
{
    // TODO: *ps will be leaked
    ca::Server *ps = new ca::Server(17000 + currentNode->iNode);
    boost::thread t(boost::bind(&ca::Server::run, ps));
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
    // for string values, set intVal as length
    for (int j = 0; j < q->nbRestrictionsEqual; ++j) {
        Value *v = &q->restrictionEqualValues[j];
        if (v->type == STRING) {
            v->intVal = std::strlen(v->charVal);
        }
    }
    for (int j = 0; j < q->nbRestrictionsGreaterThan; ++j) {
        Value *v = &q->restrictionGreaterThanValues[j];
        if (v->type == STRING) {
            v->intVal = std::strlen(v->charVal);
        }
    }

    // choose an optimal query plan and open an iterator
    conn->q = q;
    conn->root = buildQueryPlan(q);
    conn->root->Open();
}

ErrCode fetchRow(Connection *conn, Value *values)
{
    if (conn->root->GetNext(conn->tuple)) {
        conn->root->Close();
        return DB_END;
    }

    for (int i = 0; i < conn->q->nbOutputFields; ++i) {
        ca::ColID cid = conn->root->getOutputColID(conn->q->outputFields[i]);
        values[i].type = conn->root->getColType(conn->q->outputFields[i]);
        if (values[i].type == INT) {
            values[i].intVal = ca::Operator::parseInt(conn->tuple[cid].first, conn->tuple[cid].second);
#ifdef PRINT_TUPLES
            std::cout << values[i].intVal << "|";
#endif
        } else {
            std::memcpy(values[i].charVal, conn->tuple[cid].first, conn->tuple[cid].second);
            values[i].charVal[conn->tuple[cid].second] = '\0';
#ifdef PRINT_TUPLES
            std::cout << values[i].charVal << "|";
#endif
        }
    }
#ifdef PRINT_TUPLES
    std::cout << std::endl;
#endif

    return SUCCESS;
}

void closeProcess()
{
}
