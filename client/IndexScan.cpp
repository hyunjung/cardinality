#include "IndexScan.h"

using namespace ca;


IndexScan::IndexScan(const NodeID n, const char *f, const char *a,
                     const Table *t, const PartitionStats *p, const Query *q,
                     const char *col, const double o)
    : Scan(n, f, a, t, p, q),
      indexCol(), indexColType(), compOp(EQ), value(NULL), unique(),
      index(), txn(), record(),
      state(), checkIndexCond(), keyIntVal(), keyCharVal(),
      outerCardinality(o)
{
    if (col) { // NLIJ
        const char *dot = std::strchr(col, '.');
        if (dot == NULL) {
            throw std::runtime_error("invalid column name");
        }
        indexCol = table->tableName;
        indexCol += '.';
        indexCol += dot + 1;
        indexColType = getColType(col);
        unique = std::strcmp(t->fieldsName[0], dot + 1) == 0;
    } else {
        for (size_t i = 0; i < gteqConds.size(); ++i) {
            const char *col = table->fieldsName[gteqConds[i].get<0>()];
            if (col[0] == '_') {
                indexCol = table->tableName;
                indexCol += '.';
                indexCol += col;
                compOp = gteqConds[i].get<2>();
                value = gteqConds[i].get<1>();
                indexColType = value->type;
                unique = gteqConds[i].get<0>() == 0;
                gteqConds.erase(gteqConds.begin() + i);
                break;
            }
        }

        if (indexCol.empty()) {
            throw std::runtime_error("indexed column not found");
        }
    }
}

IndexScan::IndexScan()
    : indexCol(), indexColType(), compOp(), value(), unique(),
      index(), txn(), record(),
      state(), checkIndexCond(), keyIntVal(), keyCharVal(),
      outerCardinality()
{
}

IndexScan::~IndexScan()
{
}

RC IndexScan::Open(const char *leftValue, const uint32_t leftValueLen)
{
    file.open(fileName);
    openIndex(indexCol.c_str(), &index);

    record.val.type = indexColType;
    if (indexColType == INT) {
        if (leftValue) {
            keyIntVal = parseInt(leftValue, leftValueLen);
        } else {
            keyIntVal = value->intVal;
        }
        record.val.intVal = keyIntVal;
    } else { // STRING
        if (leftValue) {
            keyCharVal = leftValue;
            keyIntVal = leftValueLen;
        } else {
            keyCharVal = value->charVal;
            keyIntVal = value->intVal;
        }
        std::memcpy(record.val.charVal, keyCharVal, keyIntVal);
        record.val.charVal[keyIntVal] = '\0';
    }

    beginTransaction(&txn);
    state = INDEX_GET;
    checkIndexCond = true;

    return 0;
}

RC IndexScan::ReScan(const char *leftValue, const uint32_t leftValueLen)
{
    if (indexColType == INT) {
        if (leftValue) {
            keyIntVal = parseInt(leftValue, leftValueLen);
        }
        record.val.intVal = keyIntVal;
    } else { // STRING
        if (leftValue) {
            keyCharVal = leftValue;
            keyIntVal = leftValueLen;
        }
        std::memcpy(record.val.charVal, keyCharVal, keyIntVal);
        record.val.charVal[keyIntVal] = '\0';
    }

    state = INDEX_GET;
    checkIndexCond = true;

    return 0;
}

RC IndexScan::GetNext(Tuple &tuple)
{
    ErrCode ec;
    Tuple temp;

    switch (state) {
    case INDEX_DONE:
        return -1;

    case INDEX_GET:
        ec = get(index, txn, &record);
        state = INDEX_GETNEXT;

        // GT: the first tuple will be returned in the while loop
        switch (ec) {
        case SUCCESS:
            if (compOp == EQ) {
                if (unique) {
                    state = INDEX_DONE;
                }
                splitLine(file.begin() + record.address, file.end(), temp);

                if (execFilter(temp)) {
                    execProject(temp, tuple);
                    return 0;
                }
            }
            break;

        case KEY_NOTFOUND:
            if (compOp == EQ) {
                return -1;
            } else { // GT
                checkIndexCond = false;
            }
            break;

        default:
            break;
        }

    case INDEX_GETNEXT:
        while ((ec = getNext(index, txn, &record)) == SUCCESS) {
            if (checkIndexCond) {
                if (indexColType == INT) {
                    if (compOp == EQ) {
                        if (keyIntVal != record.val.intVal) {
                            return -1;
                        }
                    } else { // GT
                        if (keyIntVal >= record.val.intVal) {
                            continue;
                        }
                        checkIndexCond = false;
                    }
                } else { // STRING
                    if (compOp == EQ) {
                        if (record.val.charVal[keyIntVal] != '\0'
                            || std::memcmp(keyCharVal, record.val.charVal, keyIntVal)) {
                            return -1;
                        }
                    } else { // GT
                        uint32_t charValLen = std::strlen(record.val.charVal);
                        int cmp = std::memcmp(keyCharVal, record.val.charVal,
                                              std::min(keyIntVal, charValLen));
                        if (cmp > 0 || (cmp == 0 && keyIntVal >= charValLen)) {
                            continue;
                        }
                        checkIndexCond = false;
                    }
                }
            }

            splitLine(file.begin() + record.address, file.end(), temp);

            if (execFilter(temp)) {
                execProject(temp, tuple);
                return 0;
            }
        }
        break;
    }

    return -1;
}

RC IndexScan::Close()
{
    commitTransaction(txn);
    closeIndex(index);
    file.close();

    return 0;
}

void IndexScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "IndexScan@" << getNodeID() << " " << fileName << " ";
    if (unique) {
        os << "unique ";
    }
    os << indexCol << ((compOp == EQ) ? "=" : ">");
    if (!value) { // NLIJ
        os << "leftValue";
    } else if (value->type == INT) {
        os << value->intVal;
    } else { // STRING
        os << "'" << value->charVal << "'";
    }
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;
}

// Mackert and Lohman,
// Index Scans Using a Finite LRU Buffer: A Validated I/O Model,
// ACM Transactions on Database Systems, Vol. 14, No. 3, September 1989, p.411
//
static double MACKERT_LOHMAN(double T, double D, double x = 1)
{
    double Y;
    double b = 32 * 1024;
    double Dx = D * x;

    if (T <= b) {
        Y = std::min(T, (2.0 * T * Dx) / (2.0 * T + Dx));
    } else { // T > b
        double l = (2.0 * T * b) / (2.0 * T - b);
        if (Dx <= l) {
            Y = (2.0 * T * Dx) / (2.0 * T + Dx);
        } else {
            Y = b + (Dx - l) * (T - b) / T;
        }
    }

    return Y;
}

double IndexScan::estCost() const
{
    double seqPages = 0;
    double randomPages = 0;

    if (!value) { // NLIJ
        randomPages = MACKERT_LOHMAN(stats->numPages,
                                     (unique) ? 1 : 3, outerCardinality)
                      / outerCardinality;
    } else {
        if (compOp == EQ) {
            if (unique) {
                seqPages = MACKERT_LOHMAN(stats->numPages, 1);
            } else {
                randomPages = MACKERT_LOHMAN(stats->numPages, 3);
            }
        } else { // GT
            if (unique) {
                seqPages = stats->numPages * SELECTIVITY_GT;
            } else {
                randomPages = MACKERT_LOHMAN(stats->numPages,
                                             stats->cardinality * SELECTIVITY_GT);
            }
        }
    }

    return (seqPages + randomPages) * COST_DISK_READ_PAGE
           + randomPages * COST_DISK_SEEK_PAGE;
}

double IndexScan::estCardinality() const
{
    double card = stats->cardinality;

    if (compOp == EQ) {
        if (unique) {
            card /= stats->cardinality;
        } else {
            card *= SELECTIVITY_EQ;
        }
    } else { // GT
        card *= SELECTIVITY_GT;
    }

    for (size_t i = 0; i < gteqConds.size(); ++i) {
        if (gteqConds[i].get<2>() == EQ) {
            if (gteqConds[i].get<0>() == 0) {
                card /= stats->cardinality;
            } else {
                card *= SELECTIVITY_EQ;
            }
        } else { // GT
            card *= SELECTIVITY_GT;
        }
    }

    if (!joinConds.empty()) {
        card *= SELECTIVITY_EQ;
    }

    return card;
}
