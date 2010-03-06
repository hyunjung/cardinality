#include "client/optimizer.h"
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"


namespace ca = cardinality;

extern const Nodes *g_nodes;
extern std::map<std::string, Table * > g_tables;
extern std::map<std::pair<std::string, int>, ca::PartitionStats * > g_stats;


static inline bool HASIDXCOL(const char *col, const char *alias)
{
    int aliasLen = std::strlen(alias);
    return col[aliasLen] == '.' && col[aliasLen + 1] == '_'
           && !std::memcmp(col, alias, aliasLen);
}

static void buildScans(const Query *q,
                       std::vector<ca::Operator::Ptr> &plans)
{
    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = g_tables[std::string(q->tableNames[i])];
        Partition *part = &table->partitions[0];
        ca::PartitionStats *stats = g_stats[std::make_pair(std::string(q->tableNames[i]), 0)];

        plans.push_back(ca::Scan::Ptr(
                        new ca::SeqScan(part->iNode,
                                        part->fileName, q->aliasNames[i],
                                        table, stats, q)));

        try {
            plans.push_back(ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, stats, q)));
        } catch (std::runtime_error &e) {}
    }
}

static void buildJoins(const Query *q,
                       std::vector<ca::Operator::Ptr> &subPlans,
                       std::vector<ca::Operator::Ptr> &plans)
{
    ca::Operator::Ptr root;
    ca::Scan::Ptr right;

    for (size_t k = 0; k < subPlans.size(); ++k) {
        for (int i = 0; i < q->nbTable; ++i) {
            if (subPlans[k]->hasCol(q->aliasNames[i])) {
                continue;
            }

            Table *table = g_tables[std::string(q->tableNames[i])];
            Partition *part = &table->partitions[0];
            ca::PartitionStats *stats = g_stats[std::make_pair(std::string(q->tableNames[i]), 0)];
            root = subPlans[k];

            // add Remote operator if needed
            if (root->node_id() != part->iNode) {
                root = ca::Operator::Ptr(
                       new ca::Remote(part->iNode, root,
                                      g_nodes->nodes[root->node_id()].ip));
            }

            // Nested Loop Index Join
            for (int j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                    && root->hasCol(q->joinFields2[j])) {
                    right = ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, stats, q,
                                              q->joinFields1[j], root->estCardinality()));
                    plans.push_back(ca::Operator::Ptr(
                                    new ca::NLJoin(right->node_id(), root, right,
                                                   q, j, q->joinFields2[j])));
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                    && root->hasCol(q->joinFields1[j])) {
                    right = ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, stats, q,
                                              q->joinFields2[j], root->estCardinality()));
                    plans.push_back(ca::Operator::Ptr(
                                    new ca::NLJoin(right->node_id(), root, right,
                                                   q, j, q->joinFields1[j])));
                    break;
                }
            }

            // Nested Block Join
            try {
                right = ca::Scan::Ptr(
                        new ca::IndexScan(part->iNode,
                                          part->fileName, q->aliasNames[i],
                                          table, stats, q));
                plans.push_back(ca::Operator::Ptr(
                                new ca::NBJoin(right->node_id(), root, right, q)));
            } catch (std::runtime_error &e) {}

            right = ca::Scan::Ptr(
                    new ca::SeqScan(part->iNode,
                                    part->fileName, q->aliasNames[i],
                                    table, stats, q));
            plans.push_back(ca::Operator::Ptr(
                            new ca::NBJoin(right->node_id(), root, right, q)));
        }
    }
}

ca::Operator::Ptr buildQueryPlanIgnoringPartitions(const Query *q)
{
    std::vector<ca::Operator::Ptr> plans;
    std::vector<ca::Operator::Ptr> subPlans;

    // enumerate equivalent execution trees
    for (int i = 0; i < q->nbTable; ++i) {
        if (i == 0) {
            buildScans(q, plans);
        } else {
            subPlans.clear();
            plans.swap(subPlans);
            buildJoins(q, subPlans, plans);
        }
    }

    ca::Operator::Ptr bestPlan;
    double minCost = 0;

    // add Remote operator if needed
    for (size_t k = 0; k < plans.size(); ++k) {
        ca::Operator::Ptr root = plans[k];
        if (root->node_id() != 0) {
            root = ca::Operator::Ptr(
                   new ca::Remote(0, root, g_nodes->nodes[root->node_id()].ip));
            plans[k] = root;
        }

        double cost = root->estCost();
        if (k == 0 || cost < minCost) {
            minCost = cost;
            bestPlan = plans[k];
        }
    }

    bestPlan->print(std::cout);
    return bestPlan;
}

ca::Operator::Ptr buildSimpleQueryPlanIgnoringPartitions(const Query *q)
{
    ca::Operator::Ptr root;
    ca::Scan::Ptr right;

    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = g_tables[std::string(q->tableNames[i])];
        Partition *part = &table->partitions[0];

        if (i == 0) {
            try {
                right = ca::Scan::Ptr(
                        new ca::IndexScan(part->iNode,
                                          part->fileName, q->aliasNames[i],
                                          table, NULL, q));
            } catch (std::runtime_error &e) {
                right = ca::Scan::Ptr(
                        new ca::SeqScan(part->iNode,
                                        part->fileName, q->aliasNames[i],
                                        table, NULL, q));
            }
            root = right;

        } else {
            // add Remote operator if needed
            if (root->node_id() != part->iNode) {
                root = ca::Operator::Ptr(
                       new ca::Remote(part->iNode, root,
                                      g_nodes->nodes[root->node_id()].ip));
            }

            int j;

            // Nested Loop Index Join
            for (j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                    && root->hasCol(q->joinFields2[j])) {
                    right = ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, NULL, q, q->joinFields1[j]));
                    root = ca::Operator::Ptr(
                           new ca::NLJoin(right->node_id(), root, right,
                                          q, j, q->joinFields2[j]));
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                    && root->hasCol(q->joinFields1[j])) {
                    right = ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, NULL, q, q->joinFields2[j]));
                    root = ca::Operator::Ptr(
                           new ca::NLJoin(right->node_id(), root, right,
                                          q, j, q->joinFields1[j]));
                    break;
                }
            }

            // Nested Block Join
            if (j == q->nbJoins) {
                try {
                    right = ca::Scan::Ptr(
                            new ca::IndexScan(part->iNode,
                                              part->fileName, q->aliasNames[i],
                                              table, NULL, q));
                } catch (std::runtime_error &e) {
                    right = ca::Scan::Ptr(
                            new ca::SeqScan(part->iNode,
                                            part->fileName, q->aliasNames[i],
                                            table, NULL, q));
                }
                root = ca::Operator::Ptr(
                       new ca::NBJoin(right->node_id(), root, right, q));
            }
        }
    }

    if (root->node_id() != 0) {
        root = ca::Operator::Ptr(
               new ca::Remote(0, root, g_nodes->nodes[root->node_id()].ip));
    }

    return root;
}

ca::Operator::Ptr buildSimpleQueryPlanForSingleTable(const Query *q)
{
    ca::Operator::Ptr root;
    Table *table = g_tables[std::string(q->tableNames[0])];

    for (int j = 0; j < q->nbRestrictionsEqual; ++j) {
        if (!std::strcmp(q->restrictionEqualFields[j]
                         + std::strlen(q->aliasNames[0]) + 1,
                         table->fieldsName[0])) {  // primary key
            Value *v = &q->restrictionEqualValues[j];
            for (int k = 0; k < table->nbPartitions; ++k) {
                ca::PartitionStats *stats
                    = g_stats[std::make_pair(std::string(q->tableNames[0]), k)];
                if ((v->type == INT
                     && stats->min_val_.intVal <= v->intVal
                     && stats->max_val_.intVal >= v->intVal)
                    || (v->type == STRING
                        && std::strcmp(stats->min_val_.charVal, v->charVal) <= 0
                        && std::strcmp(stats->max_val_.charVal, v->charVal) >= 0)) {
                    Partition *part = &table->partitions[k];
                    root = ca::Scan::Ptr(
                           new ca::IndexScan(part->iNode,
                                             part->fileName, q->aliasNames[0],
                                             table, NULL, q));
                    break;
                }
            }

            if (root.get() == NULL) {
                Partition *part = &table->partitions[0];
                root = ca::Scan::Ptr(
                       new ca::IndexScan(part->iNode,
                                         part->fileName, q->aliasNames[0],
                                         table, NULL, q));
            }
            if (root->node_id() != 0) {
                root = ca::Operator::Ptr(
                       new ca::Remote(0, root, g_nodes->nodes[root->node_id()].ip));
            }
            break;
        }
    }

    return root;
}

static ca::Operator::Ptr buildUnion(const ca::NodeID n, std::vector<ca::Operator::Ptr> children)
{
    for (size_t k = 0; k < children.size(); ++k) {
        if (children[k]->node_id() != n) {
            children[k] = ca::Operator::Ptr(
                          new ca::Remote(n, children[k],
                                         g_nodes->nodes[children[k]->node_id()].ip));
        }
    }

    if (children.size() > 1) {
        return ca::Operator::Ptr(new ca::Union(n, children));
    } else {
        return children[0];
    }
}

ca::Operator::Ptr buildSimpleQueryPlan(const Query *q)
{
    std::vector<ca::Operator::Ptr> roots;
    std::vector<ca::Operator::Ptr> roots0;
    ca::Operator::Ptr root;
    ca::Scan::Ptr right;

    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = g_tables[std::string(q->tableNames[i])];
        int nbPartitions = 0;
        int *m = new int[table->nbPartitions];
        for (int k = 0; k < table->nbPartitions; ++k) {
            int j;
            for (j = 0; j < nbPartitions; ++j) {
                ca::PartitionStats *stats
                    = g_stats[std::make_pair(std::string(q->tableNames[i]), k)];
                ca::PartitionStats *s
                    = g_stats[std::make_pair(std::string(q->tableNames[i]), m[j])];
                if (stats->min_val_.type == s->min_val_.type
                    && stats->min_val_.intVal == s->min_val_.intVal
                    && (stats->min_val_.type == INT
                        || !memcmp(stats->min_val_.charVal,
                                   s->min_val_.charVal,
                                   s->min_val_.intVal))) {
                    break;
                }
            }
            if (j == nbPartitions) {
                m[nbPartitions++] = k;
            }
        }

        if (i == 0) {
            for (int k = 0; k < nbPartitions; ++k) {
                Partition *part = &table->partitions[m[k]];
                ca::PartitionStats *stats
                    = g_stats[std::make_pair(std::string(q->tableNames[i]), m[k])];
#ifndef DISABLE_INDEXSCAN
                try {
                    roots.push_back(ca::Scan::Ptr(
                                    new ca::IndexScan(part->iNode,
                                                      part->fileName, q->aliasNames[i],
                                                      table, stats, q)));
                } catch (std::runtime_error &e) {
#endif
                    roots.push_back(ca::Scan::Ptr(
                                    new ca::SeqScan(part->iNode,
                                                    part->fileName, q->aliasNames[i],
                                                    table, stats, q)));
#ifndef DISABLE_INDEXSCAN
                }
#endif
            }

        } else {

            roots0.clear();
            roots.swap(roots0);

            int j = q->nbJoins;

            // Nested Loop Index Join
#ifndef DISABLE_INDEXJOIN
            for (j = 0; j < q->nbJoins; ++j) {
                if (HASIDXCOL(q->joinFields1[j], q->aliasNames[i])
                    && roots0[0]->hasCol(q->joinFields2[j])) {
                    for (int k = 0; k < nbPartitions; ++k) {
                        Partition *part = &table->partitions[m[k]];
                        ca::PartitionStats *stats
                            = g_stats[std::make_pair(std::string(q->tableNames[i]), m[k])];
                        root = buildUnion(part->iNode, roots0);

                        right = ca::Scan::Ptr(
                                new ca::IndexScan(part->iNode,
                                                  part->fileName, q->aliasNames[i],
                                                  table, stats, q, q->joinFields1[j],
                                                  root->estCardinality()));
                        roots.push_back(ca::Operator::Ptr(
                                        new ca::NLJoin(right->node_id(), root, right,
                                                       q, j, q->joinFields2[j])));
                    }
                    break;
                }
                if (HASIDXCOL(q->joinFields2[j], q->aliasNames[i])
                    && roots0[0]->hasCol(q->joinFields1[j])) {
                    for (int k = 0; k < nbPartitions; ++k) {
                        Partition *part = &table->partitions[m[k]];
                        ca::PartitionStats *stats
                            = g_stats[std::make_pair(std::string(q->tableNames[i]), m[k])];
                        root = buildUnion(part->iNode, roots0);

                        right = ca::Scan::Ptr(
                                new ca::IndexScan(part->iNode,
                                                  part->fileName, q->aliasNames[i],
                                                  table, stats, q, q->joinFields2[j],
                                                  root->estCardinality()));
                        roots.push_back(ca::Operator::Ptr(
                                        new ca::NLJoin(right->node_id(), root, right,
                                                       q, j, q->joinFields1[j])));
                    }
                    break;
                }
            }
#endif

            // Nested Block Join
            if (j == q->nbJoins) {
                for (int k = 0; k < nbPartitions; ++k) {
                    Partition *part = &table->partitions[m[k]];
                    ca::PartitionStats *stats
                        = g_stats[std::make_pair(std::string(q->tableNames[i]), m[k])];
                    root = buildUnion(part->iNode, roots0);

#ifndef DISABLE_INDEXSCAN
                    try {
                        right = ca::Scan::Ptr(
                                new ca::IndexScan(part->iNode,
                                                  part->fileName, q->aliasNames[i],
                                                  table, stats, q));
                    } catch (std::runtime_error &e) {
#endif
                        right = ca::Scan::Ptr(
                                new ca::SeqScan(part->iNode,
                                                part->fileName, q->aliasNames[i],
                                                table, stats, q));
#ifndef DISABLE_INDEXSCAN
                    }
#endif
                    roots.push_back(ca::Operator::Ptr(
                                    new ca::NBJoin(right->node_id(), root->clone(), right, q)));
                }
            }
        }
    }

    root = buildUnion(0, roots);

    root->print(std::cout);
    return root;
}
