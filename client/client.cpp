#include <boost/thread.hpp>
#include "../include/client.h"
#include "Server.h"
#include "optimizer.h"


struct Connection {
    const Query *q;
    op::Operator::Ptr root;
    op::Tuple tuple;
};

const Nodes *gNodes;
std::map<std::string, Table * > gTables;
std::map<std::string, PartitionStats * > gStats;


void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes, const Data *data, const Queries *preset)
{
    gNodes = nodes;

    for (int i = 0; i < data->nbTables; ++i) {
        gTables[std::string(data->tables[i].tableName)] = &data->tables[i];
    }

    for (int i = 0; i < data->nbTables; ++i) {
//      for (int j = 0; j < data->tables[i].nbPartitions; ++j) {
        for (int j = 0; j < 1; ++j) {
            PartitionStats *stats = sampleTable(data->tables[i].partitions[j].fileName,
                                                data->tables[i].nbFields);
            gStats[std::string(data->tables[i].tableName)] = stats;
        }
    }

    // TODO: *ps will be leaked
    Server *ps = new Server(17000);
    boost::thread t(boost::bind(&Server::run, ps));
}

void startSlave(const Node *masterNode, const Node *currentNode)
{
    // TODO: *ps will be leaked
    Server *ps = new Server(17000 + currentNode->iNode);
    boost::thread t(boost::bind(&Server::run, ps));
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
        op::ColID cid = conn->root->getOutputColID(conn->q->outputFields[i]);
        values[i].type = conn->root->getColType(conn->q->outputFields[i]);
        if (values[i].type == INT) {
            values[i].intVal = static_cast<uint32_t>(std::atoi(conn->tuple[cid].first));
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
