#include <boost/thread.hpp>
#include "../include/client.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "Remote.h"
#include "Server.h"


struct Connection {
    const Query *q;
    op::Operator::Ptr root;
    op::Tuple tuple;
};

static const Nodes *sNodes;
static std::map<std::string, Table * > sTables;

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes, const Data *data, const Queries *preset)
{
    sNodes = nodes;

    for (int i = 0; i < data->nbTables; ++i) {
        sTables[std::string(data->tables[i].tableName)] = &data->tables[i];
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

static inline bool HASIDXCOL(const char *col, const char *alias)
{
    int aliasLen = strlen(alias);
    return col[aliasLen] == '.' && col[aliasLen + 1] == '_'
           && !memcmp(col, alias, aliasLen);
}

void performQuery(Connection *conn, const Query *q)
{
    conn->q = q;

    // for string values, set intVal as length
    for (int j = 0; j < q->nbRestrictionsEqual; ++j) {
        Value *v = &q->restrictionEqualValues[j];
        if (v->type == STRING) {
            v->intVal = strlen(v->charVal);
        }
    }
    for (int j = 0; j < q->nbRestrictionsGreaterThan; ++j) {
        Value *v = &q->restrictionGreaterThanValues[j];
        if (v->type == STRING) {
            v->intVal = strlen(v->charVal);
        }
    }


    op::Scan::Ptr right;

    // construct an execution tree
    for (int i = 0; i < q->nbTable; ++i) {
        std::string tableName(q->tableNames[i]);
        Table *table = sTables[tableName];
        Partition *part = &table->partitions[0];

        if (i == 0) {
            try {
                right = op::Scan::Ptr(
                        new op::IndexScan(part->iNode, part->fileName, q->aliasNames[i], table, q));
            } catch (std::runtime_error &e) {
                right = op::Scan::Ptr(
                        new op::SeqScan(part->iNode, part->fileName, q->aliasNames[i], table, q));
            }
            conn->root = right;

        } else {
            // add Remote operator if needed
            if (conn->root->getNodeID() != part->iNode) {
                conn->root = op::Operator::Ptr(
                             new op::Remote(part->iNode, conn->root, sNodes->nodes[conn->root->getNodeID()].ip));
            }

            int j;

            // NLIJ
            for (j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i]) && conn->root->hasCol(q->joinFields2[j])) {
                    right = op::Scan::Ptr(
                            new op::IndexScan(part->iNode, part->fileName, q->aliasNames[i],
                                              table, q, q->joinFields1[j]));
                    conn->root = op::Operator::Ptr(
                                 new op::NLJoin(right->getNodeID(), conn->root, right,
                                                q, j, q->joinFields2[j]));
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i]) && conn->root->hasCol(q->joinFields1[j])) {
                    right = op::Scan::Ptr(
                            new op::IndexScan(part->iNode, part->fileName, q->aliasNames[i],
                                              table, q, q->joinFields2[j]));
                    conn->root = op::Operator::Ptr(
                                 new op::NLJoin(right->getNodeID(), conn->root, right,
                                                q, j, q->joinFields1[j]));
                    break;
                }
            }

            // NBJ
            if (j == q->nbJoins) {
                try {
                    right = op::Scan::Ptr(
                            new op::IndexScan(part->iNode, part->fileName, q->aliasNames[i], table, q));
                } catch (std::runtime_error &e) {
                    right = op::Scan::Ptr(
                            new op::SeqScan(part->iNode, part->fileName, q->aliasNames[i], table, q));
                }
                conn->root = op::Operator::Ptr(
                             new op::NBJoin(right->getNodeID(), conn->root, right, q));
            }
        }
    }
    if (conn->root->getNodeID() != 0) {
        conn->root = op::Operator::Ptr(
                     new op::Remote(0, conn->root, sNodes->nodes[conn->root->getNodeID()].ip));
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
            values[i].intVal = static_cast<uint32_t>(atoi(conn->tuple[cid].first));
#ifdef PRINT_TUPLES
            std::cout << values[i].intVal << "|";
#endif
        } else {
            memcpy(values[i].charVal, conn->tuple[cid].first, conn->tuple[cid].second);
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
