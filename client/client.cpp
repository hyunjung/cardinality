#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/checked_delete.hpp>
#include <google/protobuf/io/zero_copy_stream_impl.h>
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

static boost::asio::ip::address_v4 *g_addrs;
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

static bool NO_PKEY_OVERLAP(const ca::PartitionStats *a,
                            const ca::PartitionStats *b)
{
    return a && b && (compareValue(a->min_pkey_, b->max_pkey_) > 0
                      || compareValue(a->max_pkey_, b->min_pkey_) < 0);
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

        try {
            scans.push_back(
                boost::make_shared<ca::IndexScan>(
                    part->iNode,
                    part->fileName, q->aliasNames[i],
                    table, stats, q));

        } catch (std::runtime_error &e) {
            scans.push_back(
                boost::make_shared<ca::SeqScan>(
                    part->iNode,
                    part->fileName, q->aliasNames[i],
                    table, stats, q));
        }
    }
}

static void enumerateJoins(const Query *q,
                           const std::vector<ca::Operator::Ptr> &scans,
                           const std::vector<ca::Operator::Ptr> &subplans,
                           std::vector<ca::Operator::Ptr> &plans)
{
    ca::Operator::Ptr subplan;

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
            if (subplan->node_id() != static_cast<ca::NodeID>(part->iNode)) {
                subplan = boost::make_shared<ca::Remote>(
                              part->iNode, subplan,
                              g_addrs[subplan->node_id()]);
            }

            // Nested Loop Index Join
            for (int j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                    && subplan->hasCol(q->joinFields2[j])) {
                    plans.push_back(
                        boost::make_shared<ca::NLJoin>(
                            part->iNode,
                            subplan,
                            boost::make_shared<ca::IndexScan>(
                                part->iNode,
                                part->fileName, q->aliasNames[i],
                                table, stats, q,
                                q->joinFields1[j]),
                            q, j, q->joinFields2[j]));
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                    && subplan->hasCol(q->joinFields1[j])) {
                    plans.push_back(
                        boost::make_shared<ca::NLJoin>(
                            part->iNode,
                            subplan,
                            boost::make_shared<ca::IndexScan>(
                                part->iNode,
                                part->fileName, q->aliasNames[i],
                                table, stats, q,
                                q->joinFields2[j]),
                            q, j, q->joinFields1[j]));
                    break;
                }
            }

            // Nested Block Join
            plans.push_back(
                boost::make_shared<ca::NBJoin>(
                    scans[i]->node_id(), subplan, scans[i], q));
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
            root = boost::make_shared<ca::Remote>(
                       MASTER_NODE_ID, root,
                       g_addrs[root->node_id()]);
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
    ca::Operator::Ptr right;

    std::string table_name(q->tableNames[0]);
    Table *table = g_tables[table_name];
    Partition *part = &table->partitions[0];

    try {
        root = boost::make_shared<ca::IndexScan>(
                   part->iNode,
                   part->fileName, q->aliasNames[0],
                   table,
                   static_cast<ca::PartitionStats *>(NULL), q);

    } catch (std::runtime_error &e) {
        root = boost::make_shared<ca::SeqScan>(
                   part->iNode,
                   part->fileName, q->aliasNames[0],
                   table,
                   static_cast<ca::PartitionStats *>(NULL), q);
    }

    for (int i = 1; i < q->nbTable; ++i) {
        table_name = q->tableNames[i];
        table = g_tables[table_name];
        part = &table->partitions[0];

        // add a Remote operator if needed
        if (root->node_id() != static_cast<ca::NodeID>(part->iNode)) {
            root = boost::make_shared<ca::Remote>(
                       part->iNode, root,
                       g_addrs[root->node_id()]);
        }

        int j;

        // Nested Loop Index Join
        for (j = 0; j < q->nbJoins; ++j) {
            if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                && root->hasCol(q->joinFields2[j])) {
                root = boost::make_shared<ca::NLJoin>(
                           part->iNode,
                           root,
                           boost::make_shared<ca::IndexScan>(
                               part->iNode,
                               part->fileName, q->aliasNames[i],
                               table,
                               static_cast<ca::PartitionStats *>(NULL), q,
                               q->joinFields1[j]),
                           q, j, q->joinFields2[j]);
                break;
            }
            if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                && root->hasCol(q->joinFields1[j])) {
                root = boost::make_shared<ca::NLJoin>(
                           part->iNode,
                           root,
                           boost::make_shared<ca::IndexScan>(
                               part->iNode,
                               part->fileName, q->aliasNames[i],
                               table,
                               static_cast<ca::PartitionStats *>(NULL), q,
                               q->joinFields2[j]),
                           q, j, q->joinFields1[j]);
                break;
            }
        }

        // Nested Block Join
        if (j == q->nbJoins) {
            try {
                right = boost::make_shared<ca::IndexScan>(
                            part->iNode,
                            part->fileName, q->aliasNames[i],
                            table,
                            static_cast<ca::PartitionStats *>(NULL), q);

            } catch (std::runtime_error &e) {
                right = boost::make_shared<ca::SeqScan>(
                            part->iNode,
                            part->fileName, q->aliasNames[i],
                            table,
                            static_cast<ca::PartitionStats *>(NULL), q);
            }
            root = boost::make_shared<ca::NBJoin>(
                       right->node_id(), root, right, q);
        }
    }

    if (root->node_id() != MASTER_NODE_ID) {
        root = boost::make_shared<ca::Remote>(
                   MASTER_NODE_ID, root,
                   g_addrs[root->node_id()]);
    }

    return root;
}
#endif

static void
findPartitionStats(const Query *q,
                   const std::string &table_name,
                   const char *alias_name,
                   std::vector<ca::PartitionStats *>::const_iterator &begin,
                   std::vector<ca::PartitionStats *>::const_iterator &end)
{
    Table *table = g_tables[table_name];
    int alias_len = std::strlen(alias_name);

    // scan all distinct partitions by default
    begin = g_stats[table_name].begin();
    end = g_stats[table_name].end();

    for (int j = 0; j < q->nbRestrictionsEqual; ++j) {
        // find an equality condition on the primary key
        if (q->restrictionEqualFields[j][alias_len] != '.'
            || std::strcmp(q->restrictionEqualFields[j] + alias_len + 1,
                           table->fieldsName[0])
            || std::memcmp(q->restrictionEqualFields[j],
                           alias_name, alias_len)) {
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
        std::vector<ca::PartitionStats *>::const_iterator it
            = std::upper_bound(begin, end, &key,
                               comparePartitionStats);

        if (it == begin) {
            // no partition contains this key
            end = it;
        } else {
            begin = it - 1;
            end = it;
        }
        return;
    }

    for (int j = 0; j < q->nbRestrictionsGreaterThan; ++j) {
        // find an equality condition on the primary key
        if (q->restrictionGreaterThanFields[j][alias_len] != '.'
            || std::strcmp(q->restrictionGreaterThanFields[j] + alias_len + 1,
                           table->fieldsName[0])
            || std::memcmp(q->restrictionGreaterThanFields[j],
                           alias_name, alias_len)) {
            continue;
        }

        // construct a PartitionStats object
        // in order to use comparePartitionStats()
        ca::PartitionStats key;
        key.min_pkey_.type = q->restrictionGreaterThanValues[j].type;
        key.min_pkey_.intVal = q->restrictionGreaterThanValues[j].intVal;
        if (key.min_pkey_.type == STRING) {
            std::memcpy(key.min_pkey_.charVal,
                        q->restrictionGreaterThanValues[j].charVal,
                        key.min_pkey_.intVal);
        }

        // determine a partition to scan
        std::vector<ca::PartitionStats *>::const_iterator it
            = std::upper_bound(begin, end, &key,
                               comparePartitionStats);

        if (it != begin) {
            begin = it - 1;
        }
        return;
    }
}

static ca::Operator::Ptr buildQueryPlanSingleTable(const Query *q)
{
    std::string table_name(q->tableNames[0]);
    Table *table = g_tables[table_name];

    std::vector<ca::PartitionStats *>::const_iterator it;
    std::vector<ca::PartitionStats *>::const_iterator end;
    findPartitionStats(q, table_name, q->aliasNames[0], it, end);

    ca::Operator::Ptr root;

    if (it == end) {
        // no partition contains this key
        root = boost::make_shared<ca::Dummy>(MASTER_NODE_ID);
    } else if (end == it + 1) {
        // IndexScan on primary key
        Partition *part = &table->partitions[(*it)->part_no_];
        root = boost::make_shared<ca::IndexScan>(
                   part->iNode,
                   part->fileName, q->aliasNames[0],
                   table, static_cast<ca::PartitionStats *>(NULL), q);

        // add a Remote operator if needed
        if (root->node_id() != MASTER_NODE_ID) {
            root = boost::make_shared<ca::Remote>(
                       MASTER_NODE_ID, root,
                       g_addrs[root->node_id()]);
        }
    }

    return root;
}

typedef std::vector<ca::Operator::Ptr> PartPlan;
typedef std::vector<PartPlan> Plan;

static void buildScans(const Query *q,
                       const std::string &table_name,
                       const char *alias_name,
                       Plan &right)
{
    Table *table = g_tables[table_name];

    std::vector<ca::PartitionStats *>::const_iterator it;
    std::vector<ca::PartitionStats *>::const_iterator end;
    findPartitionStats(q, table_name, alias_name, it, end);

    right.clear();

    // for each distinct partition
    for (; it != end; ++it) {
        PartPlan pp;

        // for each replica
        for (const ca::PartitionStats *stats = *it;
             stats != NULL; stats = stats->next_) {
            Partition *part = &table->partitions[stats->part_no_];
            ca::Operator::Ptr root;
            try {
                root = boost::make_shared<ca::IndexScan>(
                           part->iNode,
                           part->fileName, alias_name,
                           table, stats, q);

            } catch (std::runtime_error &e) {
                root = boost::make_shared<ca::SeqScan>(
                           part->iNode,
                           part->fileName, alias_name,
                           table, stats, q);
            }
            pp.push_back(root);
        }
        right.push_back(pp);
    }
}

static void buildIndexScans(const Query *q,
                            const std::string &table_name,
                            const char *alias_name,
                            const char *right_join_col,
                            Plan &right)
{
    Table *table = g_tables[table_name];

    std::vector<ca::PartitionStats *>::const_iterator it;
    std::vector<ca::PartitionStats *>::const_iterator end;
    findPartitionStats(q, table_name, alias_name, it, end);

    right.clear();

    // for each distinct partition
    for (; it != end; ++it) {
        PartPlan pp;

        // for each replica
        for (const ca::PartitionStats *stats = *it;
             stats != NULL; stats = stats->next_) {
            Partition *part = &table->partitions[stats->part_no_];

            pp.push_back(
                 boost::make_shared<ca::IndexScan>(
                     part->iNode,
                     part->fileName, alias_name,
                     table, stats, q,
                     right_join_col));
        }
        right.push_back(pp);
    }
}

static ca::Operator::Ptr buildUnion(const ca::NodeID n, Plan &plan)
{
    std::vector<ca::Operator::Ptr> best_pps;

    // for each distinct partition
    for (std::size_t j = 0; j < plan.size(); ++j) {
        ca::Operator::Ptr best_pp;
        double min_pp_cost = 0.0;

        // for each equivalent plan (for this partition)
        for (std::size_t jj = 0; jj < plan[j].size(); ++jj) {
            ca::Operator::Ptr root = plan[j][jj];
            if (root->node_id() != n) {
                root = boost::make_shared<ca::Remote>(
                           n, root,
                           g_addrs[root->node_id()]);
            }

            // estimate execution cost
            double cost = root->estCost();
            if (jj == 0 || cost < min_pp_cost) {
                min_pp_cost = cost;
                best_pp = root;
            }
        }
        best_pps.push_back(best_pp);
    }

    if (best_pps.size() > 1) {
        return boost::make_shared<ca::Union>(n, best_pps);
    } else {
        return best_pps[0];
    }
}

static ca::Operator::Ptr buildQueryPlan(const Query *q,
                                        const std::vector<int> &table_order)
{
    std::vector<Plan> plans;
    std::vector<Plan> subplans;

    // the first table
    const char *alias_name = q->aliasNames[table_order[0]];
    std::string table_name(q->tableNames[table_order[0]]);
    Plan scan;
    buildScans(q, table_name, alias_name, scan);
    if (scan.empty()) {
        return boost::make_shared<ca::Dummy>(MASTER_NODE_ID);
    }
    plans.push_back(scan);

    // all other tables
    for (int i = 1; i < q->nbTable; ++i) {
        subplans.clear();
        plans.swap(subplans);

        alias_name = q->aliasNames[table_order[i]];
        table_name = q->tableNames[table_order[i]];

        const char *left_join_col = NULL;
        const char *right_join_col = NULL;
        int join_cond = 0;

        // look for an index join condition
        for (join_cond = 0; join_cond < q->nbJoins; ++join_cond) {
            if (HASIDXCOL(q->joinFields1[join_cond], alias_name)
                && subplans[0][0][0]->hasCol(q->joinFields2[join_cond])) {
                left_join_col = q->joinFields2[join_cond];
                right_join_col = q->joinFields1[join_cond];
                break;
            } else if (HASIDXCOL(q->joinFields2[join_cond], alias_name)
                       && subplans[0][0][0]->hasCol(q->joinFields1[join_cond])) {
                left_join_col = q->joinFields1[join_cond];
                right_join_col = q->joinFields2[join_cond];
                break;
            }
        }

        Plan right;
        Plan right_i;

        // Nested Block Join
        buildScans(q, table_name, alias_name, right);

        // Nested Loop Index Join
        if (left_join_col) {
            buildIndexScans(q, table_name, alias_name, right_join_col, right_i);
        }

        if (right.empty()) {
            return boost::make_shared<ca::Dummy>(MASTER_NODE_ID);
        }

        // no Union
        for (std::size_t l = 0; l < subplans.size(); ++l) {
            Plan &subplan = subplans[l];
            Plan plan;

            for (std::size_t k = 0; k < right.size(); ++k) {
                for (std::size_t j = 0; j < subplan.size(); ++j) {
                    PartPlan pp;

                    // if both operands of the join condition are primary
                    // keys and two partition ranges do not overlap, skip
                    // this combination.
                    if (left_join_col
                        && right[k][0]->getBaseColID(
                               right[k][0]->getOutputColID(right_join_col)) == 0
                        && subplan[j][0]->getBaseColID(
                               subplan[j][0]->getOutputColID(left_join_col)) == 0
                        && NO_PKEY_OVERLAP(
                               subplan[j][0]->getPartitionStats(left_join_col),
                               right[k][0]->getPartitionStats(right_join_col))) {
                        continue;
                    }

                    for (std::size_t kk = 0; kk < right[k].size(); ++kk) {
                        for (std::size_t jj = 0; jj < subplan[j].size(); ++jj) {
                            ca::Operator::Ptr root = subplan[j][jj];
                            if (root->node_id() != right[k][kk]->node_id()) {
                                root = boost::make_shared<ca::Remote>(
                                           right[k][kk]->node_id(), root,
                                           g_addrs[root->node_id()]);
                            }

                            pp.push_back(
                                boost::make_shared<ca::NBJoin>(
                                    root->node_id(),
                                    root, right[k][kk], q));

                            if (left_join_col) {
                                ca::Operator::Ptr root = subplan[j][jj];
                                if (root->node_id() != right_i[k][kk]->node_id()) {
                                    root = boost::make_shared<ca::Remote>(
                                               right[k][kk]->node_id(), root,
                                               g_addrs[root->node_id()]);
                                }

                                pp.push_back(
                                    boost::make_shared<ca::NLJoin>(
                                        root->node_id(),
                                        root, right_i[k][kk],
                                        q, join_cond, left_join_col));
                            }
                        }
                    }
                    plan.push_back(pp);
                }
            }

            if (plan.empty()) {
                return boost::make_shared<ca::Dummy>(MASTER_NODE_ID);
            }
            plans.push_back(plan);
        }

        // left Union
        for (std::size_t l = 0; l < subplans.size(); ++l) {
            Plan &subplan = subplans[l];
            Plan plan;

            for (std::size_t k = 0; k < right.size(); ++k) {
                Plan left;

                for (std::size_t j = 0; j < subplan.size(); ++j) {
                    // if both operands of the join condition are primary
                    // keys and two partition ranges do not overlap, skip
                    // this combination.
                    if (left_join_col
                        && right[k][0]->getBaseColID(
                               right[k][0]->getOutputColID(right_join_col)) == 0
                        && subplan[j][0]->getBaseColID(
                               subplan[j][0]->getOutputColID(left_join_col)) == 0
                        && NO_PKEY_OVERLAP(
                               subplan[j][0]->getPartitionStats(left_join_col),
                               right[k][0]->getPartitionStats(right_join_col))) {
                        continue;
                    }
                    left.push_back(subplan[j]);
                }

                if (left.empty()) {
                    continue;
                }

                PartPlan pp;
                for (std::size_t kk = 0; kk < right[k].size(); ++kk) {
                    ca::Operator::Ptr root(
                        buildUnion(right[k][kk]->node_id(),
                                   left));
                    pp.push_back(
                        boost::make_shared<ca::NBJoin>(
                            root->node_id(),
                            root, right[k][kk], q));

                    if (left_join_col) {
                        ca::Operator::Ptr root(
                            buildUnion(right_i[k][kk]->node_id(),
                                       left));
                        pp.push_back(
                            boost::make_shared<ca::NLJoin>(
                                root->node_id(),
                                root, right_i[k][kk],
                                q, join_cond, left_join_col));
                    }
                }
                plan.push_back(pp);
            }

            if (plan.empty()) {
                return boost::make_shared<ca::Dummy>(MASTER_NODE_ID);
            }
            plans.push_back(plan);
        }

        // right Union
        for (std::size_t l = 0; l < subplans.size(); ++l) {
            Plan &subplan = subplans[l];
            Plan plan;

            for (std::size_t j = 0; j < subplan.size(); ++j) {
                Plan right1;
                Plan right1_i;

                for (std::size_t k = 0; k < right.size(); ++k) {
                    // if both operands of the join condition are primary
                    // keys and two partition ranges do not overlap, skip
                    // this combination.
                    if (left_join_col
                        && right[k][0]->getBaseColID(
                               right[k][0]->getOutputColID(right_join_col)) == 0
                        && subplan[j][0]->getBaseColID(
                               subplan[j][0]->getOutputColID(left_join_col)) == 0
                        && NO_PKEY_OVERLAP(
                               subplan[j][0]->getPartitionStats(left_join_col),
                               right[k][0]->getPartitionStats(right_join_col))) {
                        continue;
                    }
                    right1.push_back(right[k]);
                    if (left_join_col) {
                        right1_i.push_back(right_i[k]);
                    }
                }

                if (right1.empty()) {
                    continue;
                }

                PartPlan pp;
                for (std::size_t jj = 0; jj < subplan[j].size(); ++jj) {
                    ca::Operator::Ptr root(
                        buildUnion(subplan[j][jj]->node_id(),
                                   right1));
                    pp.push_back(
                        boost::make_shared<ca::NBJoin>(
                            subplan[j][jj]->node_id(),
                            subplan[j][jj], root, q));

                    if (left_join_col) {
                        ca::Operator::Ptr root(
                            buildUnion(subplan[j][jj]->node_id(),
                                       right1_i));
                        pp.push_back(
                            boost::make_shared<ca::NLJoin>(
                                subplan[j][jj]->node_id(),
                                subplan[j][jj], root,
                                q, join_cond, left_join_col));
                    }
                }
                plan.push_back(pp);
            }
            plans.push_back(plan);
        }
    }

    ca::Operator::Ptr best_plan;
    double min_cost = 0.0;

    for (std::size_t l = 0; l < plans.size(); ++l) {
        ca::Operator::Ptr root(
            buildUnion(MASTER_NODE_ID, plans[l]));

        double cost = root->estCost();
        if (l == 0 || cost < min_cost) {
            min_cost = cost;
            best_plan = root;
        }
    }

    return best_plan->clone();
}

static void startPreTreatmentSlave(const ca::NodeID n, const Data *data)
{
    ca::tcpsocket_ptr socket;
    for (int attempt = 0; attempt < 20; ++attempt) {
        try {
            socket = g_server->connectSocket(n, g_addrs[n]);
            break;
        } catch (...) {}

        usleep(100000);  // 0.1s
    }

    boost::asio::streambuf buf;

    uint8_t *target = boost::asio::buffer_cast<uint8_t *>(buf.prepare(1));
    target = CodedOutputStream::WriteRawToArray("S", 1, target);
    buf.commit(1);

    for (int i = 0; i < data->nbTables; ++i) {
        for (int j = 0; j < data->tables[i].nbPartitions; ++j) {
            if (data->tables[i].partitions[j].iNode != n) {
                continue;
            }

            // compute a request body size
            int len = std::strlen(data->tables[i].partitions[j].fileName);
            uint32_t size = CodedOutputStream::VarintSize32(
                                data->tables[i].nbFields)
                            + 1 + CodedOutputStream::VarintSize32(len) + len;

            // send a request
            google::protobuf::io::ArrayOutputStream aos(
                boost::asio::buffer_cast<char *>(buf.prepare(size + 4)), size + 4);
            google::protobuf::io::CodedOutputStream cos(&aos);

            cos.WriteLittleEndian32(size);  // header
            cos.WriteVarint32(data->tables[i].nbFields);
            cos.WriteVarint32(data->tables[i].fieldsType[0]);
            cos.WriteVarint32(len);
            cos.WriteRaw(data->tables[i].partitions[j].fileName, len);
            buf.commit(size + 4);

            boost::asio::write(*socket, buf);
            buf.consume(size + 4);

            // receive a response header
            unsigned char header[4];
            boost::asio::read(*socket, boost::asio::buffer(header));
            google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(
                &header[0], &size);

            // receive a response body
            boost::asio::read(*socket, buf.prepare(size));
            buf.commit(size);

            google::protobuf::io::ArrayInputStream ais(
                boost::asio::buffer_cast<const char *>(buf.data()), size);
            google::protobuf::io::CodedInputStream cis(&ais);

            ca::PartitionStats *stats = new ca::PartitionStats();
            stats->Deserialize(&cis);
            stats->part_no_ = j;

            buf.consume(size);

            // store the stats
            boost::mutex::scoped_lock lock(g_stats_mutex);
            g_stats[std::string(data->tables[i].tableName)].push_back(stats);
        }
    }

    // end of requests
    target = boost::asio::buffer_cast<uint8_t *>(buf.prepare(4));
    target = CodedOutputStream::WriteLittleEndian32ToArray(0xffffffff, target);
    buf.commit(4);
    boost::asio::write(*socket, buf);

    g_server->closeSocket(n, socket);
}

void startPreTreatmentMaster(int nbSeconds, const Nodes *nodes,
                             const Data *data, const Queries *preset)
{
    g_addrs = new boost::asio::ip::address_v4[nodes->nbNodes];

    for (int n = 0; n < nodes->nbNodes; ++n) {
        g_addrs[n] = boost::asio::ip::address_v4::from_string(
                         nodes->nodes[n].ip);
    }

    usleep(50000);  // 0.05s

    g_server = new ca::Server(17000);

    boost::thread_group threads;
    for (int n = 1; n < nodes->nbNodes; ++n) {
        threads.create_thread(boost::bind(&startPreTreatmentSlave, n, data));
    }

    for (int i = 0; i < data->nbTables; ++i) {
        std::string table_name(data->tables[i].tableName);
        Table *table = &data->tables[i];
        g_tables[table_name] = table;

        for (int j = 0; j < table->nbPartitions; ++j) {
            if (static_cast<ca::NodeID>(table->partitions[j].iNode)
                != MASTER_NODE_ID) {
                continue;
            }
            ca::PartitionStats *stats
                = new ca::PartitionStats(table->partitions[j].fileName,
                                         table->nbFields,
                                         table->fieldsType[0],
                                         j);

            boost::mutex::scoped_lock lock(g_stats_mutex);
            g_stats[table_name].push_back(stats);
        }
    }

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
            std::vector<int> join_order;
            for (int i = 0; i < q->nbTable; ++i) {
                join_order.push_back(i);
            }
            do {
                ca::Operator::Ptr root(
                    buildQueryPlan(q, join_order));

                if (!conn->root.get() || conn->root->estCost() > root->estCost()) {
                    conn->root = root;
                }
            } while (std::next_permutation(join_order.begin(), join_order.end()));
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
    // free PartitionStats objects
    std::map<std::string, std::vector<ca::PartitionStats *> >::iterator table_it;
    for (table_it = g_stats.begin(); table_it != g_stats.end(); ++table_it) {
        std::for_each(table_it->second.begin(), table_it->second.end(),
                      boost::checked_delete<ca::PartitionStats>);
    }

    g_stats.clear();
    g_tables.clear();

    delete [] g_addrs;
}
