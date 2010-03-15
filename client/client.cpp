#include <iomanip>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "include/client.h"
#include "client/Server.h"
#include "client/PartitionStats.h"
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"
#include "client/Dummy.h"


namespace ca = cardinality;

struct Connection {
    const Query *q;
    ca::Operator::Ptr root;
    ca::Tuple tuple;
    std::vector<ca::ColID> output_col_ids;
    std::vector<bool> value_types;
};

// on master
static const ca::NodeID MASTER_NODE_ID = 0;

static const Nodes *g_nodes;
static std::map<std::string, Table *> g_tables;
static std::map<std::string, std::vector<ca::PartitionStats *> > g_stats;
static boost::mutex g_stats_mutex;

// on both master and slave
ca::Server *g_server;


static inline bool HASIDXCOL(const char *col, const char *alias)
{
    int aliasLen = std::strlen(alias);
    return col[aliasLen] == '.' && col[aliasLen + 1] == '_'
           && !std::memcmp(col, alias, aliasLen);
}

static int compareValue(const Value &a, const Value &b)
{
    if (a.type == INT) {
        return a.intVal - b.intVal;
    } else {  // STRING
        int cmp = std::memcmp(a.charVal, b.charVal,
                              std::min(a.intVal, b.intVal));
        if (cmp == 0) {
            cmp = a.intVal - b.intVal;
        }
        return cmp;
    }
}

static bool comparePartitionStats(const ca::PartitionStats *a,
                                  const ca::PartitionStats *b)
{
    return compareValue(a->min_pkey_, b->min_pkey_) < 0;
}

#ifndef DISABLE_QUERY_OPTIMIZER
static void enumerateScans(const Query *q,
                           std::vector<ca::Operator::Ptr> &scans)
{
    for (int i = 0; i < q->nbTable; ++i) {
        std::string table_name(q->tableNames[i]);
        Table *table = g_tables[table_name];
        Partition *part = &table->partitions[0];
        ca::PartitionStats *stats = g_stats[table_name][0];

        ca::Scan::Ptr seqscan(
            new ca::SeqScan(part->iNode,
                            part->fileName,
                            q->aliasNames[i],
                            table, stats, q));

        try {
            ca::Scan::Ptr indexscan(
                new ca::IndexScan(part->iNode,
                                  part->fileName,
                                  q->aliasNames[i],
                                  table, stats, q));

            if (indexscan->estCost() < seqscan->estCost()) {
                scans.push_back(indexscan);
            } else {
                scans.push_back(seqscan);
            }

        } catch (std::runtime_error &e) {
            scans.push_back(seqscan);
        }
    }
}

static void enumerateJoins(const Query *q,
                           const std::vector<ca::Operator::Ptr> &scans,
                           const std::vector<ca::Operator::Ptr> &subplans,
                           std::vector<ca::Operator::Ptr> &plans)
{
    ca::Operator::Ptr subplan;
    ca::Operator::Ptr right;

    for (std::size_t k = 0; k < subplans.size(); ++k) {
        for (int i = 0; i < q->nbTable; ++i) {
            if (subplans[k]->hasCol(q->aliasNames[i])) {
                continue;
            }

            std::string table_name(q->tableNames[i]);
            Table *table = g_tables[table_name];
            Partition *part = &table->partitions[0];
            ca::PartitionStats *stats = g_stats[table_name][0];
            subplan = subplans[k];

            // add a Remote operator if needed
            if (subplan->node_id() != part->iNode) {
                subplan.reset(
                    new ca::Remote(part->iNode, subplan,
                                   g_nodes->nodes[subplan->node_id()].ip));
            }

            // Nested Loop Index Join
            for (int j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                    && subplan->hasCol(q->joinFields2[j])) {
                    right.reset(
                        new ca::IndexScan(part->iNode,
                                          part->fileName,
                                          q->aliasNames[i],
                                          table, stats, q,
                                          q->joinFields1[j]));
                    plans.push_back(ca::Operator::Ptr(
                        new ca::NLJoin(right->node_id(),
                                       subplan, right,
                                       q, j, q->joinFields2[j])));
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                    && subplan->hasCol(q->joinFields1[j])) {
                    right.reset(
                        new ca::IndexScan(part->iNode,
                                          part->fileName,
                                          q->aliasNames[i],
                                          table, stats, q,
                                          q->joinFields2[j]));
                    plans.push_back(ca::Operator::Ptr(
                        new ca::NLJoin(right->node_id(),
                                       subplan, right,
                                       q, j, q->joinFields1[j])));
                    break;
                }
            }

            // Nested Block Join
            plans.push_back(ca::Operator::Ptr(
                new ca::NBJoin(scans[i]->node_id(), subplan, scans[i], q)));
        }
    }
}

static ca::Operator::Ptr buildQueryPlanOnePartPerTable(const Query *q)
{
    std::vector<ca::Operator::Ptr> scans;
    enumerateScans(q, scans);

    if (q->nbTable == 1) {
        return scans[0];
    }

    std::vector<ca::Operator::Ptr> plans(scans);
    std::vector<ca::Operator::Ptr> subplans;

    // enumerate equivalent execution trees
    for (int i = 1; i < q->nbTable; ++i) {
        subplans.clear();
        plans.swap(subplans);
        enumerateJoins(q, scans, subplans, plans);
    }

    ca::Operator::Ptr best_plan;
    double min_cost = 0.0;

    for (std::size_t k = 0; k < plans.size(); ++k) {
        ca::Operator::Ptr root = plans[k];

        // add a Remote operator if needed
        if (root->node_id() != MASTER_NODE_ID) {
            root.reset(
                new ca::Remote(MASTER_NODE_ID, root,
                               g_nodes->nodes[root->node_id()].ip));
            plans[k] = root;
        }

        // estimate execution cost
        double cost = root->estCost();
        if (k == 0 || cost < min_cost) {
            min_cost = cost;
            best_plan = plans[k];
        }
    }

    return best_plan;
}
#else  // DISABLE_QUERY_OPTIMIZER
static ca::Operator::Ptr buildQueryPlanOnePartPerTable(const Query *q)
{
    ca::Operator::Ptr root;
    ca::Scan::Ptr right;

    std::string table_name(q->tableNames[0]);
    Table *table = g_tables[table_name];
    Partition *part = &table->partitions[0];

    try {
        root.reset(
            new ca::IndexScan(part->iNode,
                              part->fileName,
                              q->aliasNames[0],
                              table, NULL, q));
    } catch (std::runtime_error &e) {
        root.reset(
            new ca::SeqScan(part->iNode,
                            part->fileName,
                            q->aliasNames[0],
                            table, NULL, q));
    }

    for (int i = 1; i < q->nbTable; ++i) {
        table_name = q->tableNames[i];
        table = g_tables[table_name];
        part = &table->partitions[0];

        // add a Remote operator if needed
        if (root->node_id() != part->iNode) {
            root.reset(
                new ca::Remote(part->iNode, root,
                               g_nodes->nodes[root->node_id()].ip));
        }

        int j;

        // Nested Loop Index Join
        for (j = 0; j < q->nbJoins; ++j) {
            if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                && root->hasCol(q->joinFields2[j])) {
                right.reset(
                    new ca::IndexScan(part->iNode,
                                      part->fileName,
                                      q->aliasNames[i],
                                      table, NULL, q,
                                      q->joinFields1[j]));
                root.reset(
                    new ca::NLJoin(right->node_id(), root, right,
                                   q, j, q->joinFields2[j]));
                break;
            }
            if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                && root->hasCol(q->joinFields1[j])) {
                right.reset(
                    new ca::IndexScan(part->iNode,
                                      part->fileName,
                                      q->aliasNames[i],
                                      table, NULL, q,
                                      q->joinFields2[j]));
                root.reset(
                    new ca::NLJoin(right->node_id(), root, right,
                                   q, j, q->joinFields1[j]));
                break;
            }
        }

        // Nested Block Join
        if (j == q->nbJoins) {
            try {
                right.reset(
                    new ca::IndexScan(part->iNode,
                                      part->fileName,
                                      q->aliasNames[i],
                                      table, NULL, q));
            } catch (std::runtime_error &e) {
                right.reset(
                    new ca::SeqScan(part->iNode,
                                    part->fileName,
                                    q->aliasNames[i],
                                    table, NULL, q));
            }
            root.reset(
                new ca::NBJoin(right->node_id(), root, right, q));
        }
    }

    if (root->node_id() != MASTER_NODE_ID) {
        root.reset(
            new ca::Remote(MASTER_NODE_ID, root,
                           g_nodes->nodes[root->node_id()].ip));
    }

    return root;
}
#endif

static ca::Operator::Ptr buildQueryPlanSingleTable(const Query *q)
{
    std::string table_name(q->tableNames[0]);
    Table *table = g_tables[table_name];
    ca::Operator::Ptr root;

    for (int j = 0; j < q->nbRestrictionsEqual; ++j) {
        // find an equality condition on the primary key
        if (!!std::strcmp(q->restrictionEqualFields[j]
                          + std::strlen(q->aliasNames[0]) + 1,
                          table->fieldsName[0])) {
            continue;
        }

        // construct a PartitionStats object
        // in order to use comparePartitionStats()
        ca::PartitionStats key;
        key.min_pkey_.type = q->restrictionEqualValues[j].type;
        key.min_pkey_.intVal = q->restrictionEqualValues[j].intVal;
        if (key.min_pkey_.type == STRING) {
            std::memcpy(key.min_pkey_.charVal,
                        q->restrictionEqualValues[j].charVal,
                        key.min_pkey_.intVal);
        }

        // determine a partition to scan
        std::vector<ca::PartitionStats *>::iterator it
            = std::upper_bound(g_stats[table_name].begin(),
                               g_stats[table_name].end(),
                               &key,
                               comparePartitionStats);

        if (it == g_stats[table_name].begin()) {
            // no partition contains this key
            root.reset(
                new ca::Dummy(MASTER_NODE_ID));
        } else {
            // IndexScan on primary key
            Partition *part = &table->partitions[(*--it)->part_no_];
            root.reset(
                new ca::IndexScan(part->iNode,
                                  part->fileName, q->aliasNames[0],
                                  table, NULL, q));

            // add a Remote operator if needed
            if (root->node_id() != MASTER_NODE_ID) {
                root.reset(
                    new ca::Remote(MASTER_NODE_ID, root,
                                   g_nodes->nodes[root->node_id()].ip));
            }
        }

        break;
    }

    return root;
}

static ca::Operator::Ptr buildQueryPlan(const Query *q)
{
    return ca::Operator::Ptr(new ca::Dummy(MASTER_NODE_ID));
}

static void startPreTreatmentSlave(const ca::NodeID n, const Data *data)
{
    boost::asio::ip::tcp::iostream tcpstream;
    for (int attempt = 0; attempt < 20; ++attempt) {
        tcpstream.connect(g_nodes->nodes[n].ip,
                          boost::lexical_cast<std::string>(17000 + n));
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
            g_stats[std::string(data->tables[i].tableName)].push_back(stats);
        }
    }

    tcpstream.close();
}

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes,
                             const Data *data, const Queries *preset)
{
    g_nodes = nodes;

    usleep(50000);  // 0.05s

    boost::thread_group threads;
    for (int n = 1; n < g_nodes->nbNodes; ++n) {
        threads.create_thread(boost::bind(&startPreTreatmentSlave, n, data));
    }

    for (int i = 0; i < data->nbTables; ++i) {
        std::string table_name(data->tables[i].tableName);
        Table *table = &data->tables[i];
        g_tables[table_name] = table;

        for (int j = 0; j < table->nbPartitions; ++j) {
            if (table->partitions[j].iNode != MASTER_NODE_ID) {
                continue;
            }
            ca::PartitionStats *stats
                = new ca::PartitionStats(j,
                                         table->partitions[j].fileName,
                                         table->nbFields,
                                         table->fieldsType[0]);

            boost::mutex::scoped_lock lock(g_stats_mutex);
            g_stats[table_name].push_back(stats);
        }
    }

    g_server = new ca::Server(17000);
    boost::thread t(boost::bind(&ca::Server::run, g_server));

    threads.join_all();

    // for each table with 2 or more partitions
    std::map<std::string, std::vector<ca::PartitionStats *> >::iterator table_it;
    for (table_it = g_stats.begin(); table_it != g_stats.end(); ++table_it) {
        if (table_it->second.size() == 1) {
            continue;
        }

        // sort all partitions based on the minimum primary keys
        std::sort(table_it->second.begin(), table_it->second.end(),
                  comparePartitionStats);

        // chain replicas
        std::vector<ca::PartitionStats *>::iterator part_it
            = table_it->second.begin();
        std::vector<ca::PartitionStats *>::iterator unique_part_it
            = table_it->second.begin();

        while (++part_it != table_it->second.end()) {
            if (!compareValue((*unique_part_it)->min_pkey_,
                              (*part_it)->min_pkey_)) {
                (*part_it)->next_ = (*unique_part_it)->next_;
                (*unique_part_it)->next_ = *part_it;
            } else {
                *(++unique_part_it) = *part_it;
            }
        }

        table_it->second.resize(++unique_part_it - table_it->second.begin());
    }
}

void startSlave(const Node *masterNode, const Node *currentNode)
{
    g_server = new ca::Server(17000 + currentNode->iNode);
    boost::thread t(boost::bind(&ca::Server::run, g_server));
}

Connection *createConnection()
{
    Connection *conn = new Connection();
    conn->tuple.reserve(10);
    conn->output_col_ids.reserve(10);
    conn->value_types.reserve(10);
    return conn;
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

    int maxNumParts = 0;
    for (int i = 0; i < q->nbTable; ++i) {
        maxNumParts = std::max(maxNumParts,
                               g_tables[std::string(q->tableNames[i])]->nbPartitions);
    }

    if (maxNumParts == 1) {
        conn->root = buildQueryPlanOnePartPerTable(q);
    } else {
        conn->root.reset();
        if (q->nbTable == 1) {
            conn->root = buildQueryPlanSingleTable(q);
        }
        if (!conn->root.get()) {
            conn->root = buildQueryPlan(q);
        }
    }

    conn->output_col_ids.clear();
    conn->value_types.clear();

    for (int i = 0; i < q->nbOutputFields; ++i) {
        conn->output_col_ids.push_back(
            conn->root->getOutputColID(q->outputFields[i]));
        conn->value_types.push_back(
            conn->root->getColType(q->outputFields[i]));
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
        ca::ColID cid = conn->output_col_ids[i];
        if (!conn->value_types[i]) {  // INT
            values[i].type = INT;
            values[i].intVal = ca::Operator::parseInt(conn->tuple[cid].first,
                                                      conn->tuple[cid].second);
#ifdef PRINT_TUPLES
            std::cout << values[i].intVal << "|";
#endif
        } else {  // STRING
            values[i].type = STRING;
            std::memcpy(values[i].charVal,
                        conn->tuple[cid].first,
                        conn->tuple[cid].second);
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
