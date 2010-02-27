#include <boost/filesystem/operations.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include "optimizer.h"
#include "SeqScan.h"
#include "IndexScan.h"
#include "NLJoin.h"
#include "NBJoin.h"
#include "Remote.h"


extern const Nodes *gNodes;
extern std::map<std::string, Table * > gTables;
extern std::map<std::string, PartitionStats * > gStats;

PartitionStats *sampleTable(const std::string fileName, const int numInputCols)
{
    PartitionStats *stats = new PartitionStats(numInputCols);

    size_t fileSize = boost::filesystem::file_size(fileName);
    stats->numPages = (fileSize + PAGE_SIZE - 1) / PAGE_SIZE;

    size_t sampleSize = ((numInputCols + 3) / 4) * PAGE_SIZE;
    boost::iostreams::mapped_file_source file(fileName,
                                              std::min(sampleSize, fileSize));

    size_t numTuples = 0;
    int i = 0;
    for (const char *pos = file.begin(); pos < file.end(); ++numTuples, i = 0) {
        for ( ;i < numInputCols; ++i) {
            const char *delim = static_cast<const char *>(
                                    std::memchr(pos, (i == numInputCols - 1) ? '\n' : '|',
                                                file.end() - pos));
            if (delim == NULL) {
                pos = file.end();
                break;
            }
            stats->colLengths[i] += delim - pos + 1;
            pos = delim + 1;
        }
    }

    file.close();

    double tupleLength = 0;
    for (int j = 0; j < numInputCols; ++j) {
        stats->colLengths[j] /= (j < i) ? (numTuples + 1) : numTuples;
        tupleLength += stats->colLengths[j];
    }
    stats->cardinality = fileSize / tupleLength;

    return stats;
}

static void buildScans(const Query *q,
                       std::vector<ca::Operator::Ptr> &plans)
{
    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = gTables[std::string(q->tableNames[i])];
        Partition *part = &table->partitions[0];
        PartitionStats *stats = gStats[std::string(q->tableNames[i])];

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

static inline bool HASIDXCOL(const char *col, const char *alias)
{
    int aliasLen = std::strlen(alias);
    return col[aliasLen] == '.' && col[aliasLen + 1] == '_'
           && !std::memcmp(col, alias, aliasLen);
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

            Table *table = gTables[std::string(q->tableNames[i])];
            Partition *part = &table->partitions[0];
            PartitionStats *stats = gStats[std::string(q->tableNames[i])];
            root = subPlans[k];

            // add Remote operator if needed
            if (root->getNodeID() != part->iNode) {
                root = ca::Operator::Ptr(
                       new ca::Remote(part->iNode, root,
                                      gNodes->nodes[root->getNodeID()].ip));
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
                                    new ca::NLJoin(right->getNodeID(), root, right,
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
                                    new ca::NLJoin(right->getNodeID(), root, right,
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
                                new ca::NBJoin(right->getNodeID(), root, right, q)));
            } catch (std::runtime_error &e) {}

            right = ca::Scan::Ptr(
                    new ca::SeqScan(part->iNode,
                                    part->fileName, q->aliasNames[i],
                                    table, stats, q));
            plans.push_back(ca::Operator::Ptr(
                            new ca::NBJoin(right->getNodeID(), root, right, q)));
        }
    }
}

#ifndef DISABLE_JOIN_REORDERING
ca::Operator::Ptr buildQueryPlan(const Query *q)
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
        if (root->getNodeID() != 0) {
            root = ca::Operator::Ptr(
                   new ca::Remote(0, root, gNodes->nodes[root->getNodeID()].ip));
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
#else
ca::Operator::Ptr buildQueryPlan(const Query *q)
{
    ca::Operator::Ptr root;
    ca::Scan::Ptr right;

    for (int i = 0; i < q->nbTable; ++i) {
        Table *table = gTables[std::string(q->tableNames[i])];
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
            if (root->getNodeID() != part->iNode) {
                root = ca::Operator::Ptr(
                       new ca::Remote(part->iNode, root,
                                      gNodes->nodes[root->getNodeID()].ip));
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
                           new ca::NLJoin(right->getNodeID(), root, right,
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
                           new ca::NLJoin(right->getNodeID(), root, right,
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
                       new ca::NBJoin(right->getNodeID(), root, right, q));
            }
        }
    }

    if (root->getNodeID() != 0) {
        root = ca::Operator::Ptr(
               new ca::Remote(0, root, gNodes->nodes[root->getNodeID()].ip));
    }

    return root;
}
#endif
