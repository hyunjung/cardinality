#include <iomanip>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "include/client.h"
#include "client/Server.h"
#include "client/PartitionStats.h"
#include "client/optimizer.h"


namespace ca = cardinality;

struct Connection {
    const Query *q;
    ca::Operator::Ptr root;
    ca::Tuple tuple;
};

// on master
const Nodes *g_nodes;
std::map<std::string, Table *> g_tables;
std::map<std::pair<std::string, int>, ca::PartitionStats *> g_stats;
std::map<std::string, std::vector<ca::PartitionStats *> > g_stats2;
static boost::mutex g_stats_mutex;

// on both master and slave
ca::Server *g_server;


static void startPreTreatmentSlave(const ca::NodeID n, const Data *data)
{
    boost::asio::ip::tcp::iostream tcpstream;
    for (int attempt = 0; attempt < 20; ++attempt) {
        tcpstream.connect(g_nodes->nodes[n].ip, boost::lexical_cast<std::string>(17000 + n));
        if (tcpstream.good()) {
            break;
        }
        tcpstream.clear();
        usleep(100000);  // 0.1s
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

            tcpstream << std::setw(3) << std::hex << data->tables[i].nbFields;
            tcpstream << std::setw(1) << std::hex << data->tables[i].fieldsType[0];
            tcpstream << std::setw(1) << std::hex << j;
            tcpstream << data->tables[i].partitions[j].fileName << std::endl;

            boost::archive::binary_iarchive ia(tcpstream);
            ia.register_type(static_cast<ca::PartitionStats *>(NULL));

            ca::PartitionStats *stats;
            ia >> stats;

            boost::mutex::scoped_lock lock(g_stats_mutex);
            g_stats[std::make_pair(std::string(data->tables[i].tableName), j)] = stats;
            g_stats2[std::string(data->tables[i].tableName)].push_back(stats);
        }
    }

    tcpstream.close();
}

static bool compareValue(const Value &a, const Value &b)
{
    if (a.type == INT) {
        return a.intVal < b.intVal;
    } else {  // STRING
        int cmp = memcmp(a.charVal, b.charVal,
                         std::min(a.intVal, b.intVal));
        return cmp < 0 || (cmp == 0 && a.intVal < b.intVal);
    }
}

bool comparePartitionStats(const ca::PartitionStats *a,
                           const ca::PartitionStats *b)
{
    return compareValue(a->min_pkey_, b->min_pkey_);
}

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes,
                             const Data *data, const Queries *preset)
{
    g_nodes = nodes;

    for (int i = 0; i < data->nbTables; ++i) {
        g_tables[std::string(data->tables[i].tableName)] = &data->tables[i];
    }

    usleep(50000);  // 0.05s

    boost::thread_group threads;
    for (int n = 1; n < g_nodes->nbNodes; ++n) {
        threads.create_thread(boost::bind(&startPreTreatmentSlave, n, data));
    }

    for (int i = 0; i < data->nbTables; ++i) {
        for (int j = 0; j < data->tables[i].nbPartitions; ++j) {
            if (data->tables[i].partitions[j].iNode) {
                continue;
            }
            ca::PartitionStats *stats
                = new ca::PartitionStats(j,
                                         data->tables[i].partitions[j].fileName,
                                         data->tables[i].nbFields,
                                         data->tables[i].fieldsType[0]);

            boost::mutex::scoped_lock lock(g_stats_mutex);
            g_stats[std::make_pair(std::string(data->tables[i].tableName), j)] = stats;
            g_stats2[std::string(data->tables[i].tableName)].push_back(stats);
        }
    }

    g_server = new ca::Server(17000);
    boost::thread t(boost::bind(&ca::Server::run, g_server));

    threads.join_all();

    std::map<std::string, std::vector<ca::PartitionStats *> >::iterator it;
    for (it = g_stats2.begin(); it != g_stats2.end(); ++it) {
        if (it->second.size() > 1) {
            sort(it->second.begin(), it->second.end(), comparePartitionStats);
        }
    }
}

void startSlave(const Node *masterNode, const Node *currentNode)
{
    g_server = new ca::Server(17000 + currentNode->iNode);
    boost::thread t(boost::bind(&ca::Server::run, g_server));
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

    int maxParts = 0;
    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = g_tables[std::string(q->tableNames[i])];
        maxParts = std::max(maxParts, table->nbPartitions);
    }

    if (maxParts == 1) {
        conn->root = buildQueryPlanIgnoringPartitions(q);
    } else {
        conn->root.reset();
        if (q->nbTable == 1) {
            conn->root = buildSimpleQueryPlanForSingleTable(q);
        }
        if (!conn->root.get()) {
            conn->root = buildSimpleQueryPlan(q);
        }
    }

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
            values[i].intVal = ca::Operator::parseInt(conn->tuple[cid].first,
                                                      conn->tuple[cid].second);
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
