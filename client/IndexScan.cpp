#include "IndexScan.h"

using namespace op;


IndexScan::IndexScan(const NodeID n, const char *f, const char *a,
                     const Table *t, const Query *q, const char *col)
    : Scan(n, f, a, t, q), indexCol(), indexColType(), compOp(EQ), value(NULL), unique(false),
      index(NULL), txn(NULL), record(),
      state(), checkIndexCond(), keyIntVal(0), keyCharVal(NULL)
{
    if (col) { // NLIJ
        const char *dot = strchr(col, '.');
        if (dot == NULL) {
            throw std::runtime_error("invalid column name");
        }
        indexCol = table->tableName;
        indexCol += '.';
        indexCol += dot + 1;
        indexColType = getColType(col);
        unique = strcmp(t->fieldsName[0], dot + 1) == 0;
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
    : indexCol(), indexColType(), compOp(EQ), value(NULL), unique(false),
      index(NULL), txn(NULL), record(),
      state(), checkIndexCond(), keyIntVal(0), keyCharVal(NULL)
{
}

IndexScan::~IndexScan()
{
}

RC IndexScan::Open(const char *leftValue)
{
    file.open(fileName);
    openIndex(indexCol.c_str(), &index);

    record.val.type = indexColType;
    if (indexColType == INT) {
        keyIntVal = (leftValue) ? static_cast<uint32_t>(atoi(leftValue)) : value->intVal;
        record.val.intVal = keyIntVal;
    } else { // STRING
        keyCharVal = (leftValue) ? leftValue : value->charVal;
        strcpy(record.val.charVal, keyCharVal);
    }

    beginTransaction(&txn);
    state = INDEX_GET;
    checkIndexCond = true;

    return 0;
}

RC IndexScan::ReScan(const char *leftValue)
{
    commitTransaction(txn);

    record.val.type = indexColType;
    if (indexColType == INT) {
        keyIntVal = (leftValue) ? static_cast<uint32_t>(atoi(leftValue)) : value->intVal;
        record.val.intVal = keyIntVal;
    } else { // STRING
        keyCharVal = (leftValue) ? leftValue : value->charVal;
        strcpy(record.val.charVal, keyCharVal);
    }

    beginTransaction(&txn);
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
                splitLine(file.begin() + record.address, lineBuffer.get(), temp);

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
                int cmp = (indexColType == INT) ?
                          (keyIntVal - record.val.intVal) :
                          (strcmp(keyCharVal, record.val.charVal));

                if (compOp == EQ) {
                    if (cmp != 0) {
                        return -1;
                    }
                } else { // GT
                    if (cmp >= 0) {
                        continue;
                    }
                    checkIndexCond = false;
                }
            }

            splitLine(file.begin() + record.address, lineBuffer.get(), temp);

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
        os << value->charVal;
    }
    os << std::endl;
}
