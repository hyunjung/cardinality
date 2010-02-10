#include "IndexScan.h"

using namespace op;


IndexScan::IndexScan(const NodeID n, const char *f, const char *a, const Table *t, const Query *q)
    : Scan(n, f, a, t, q), indexCol(), compOp(EQ), value(NULL),
      index(NULL), txn(NULL), record(), getNotCalled(), checkIndexCond()
{
    for (size_t i = 0; i < gteqConds.size(); ++i) {
        const char *col = table->fieldsName[gteqConds[i].get<0>()];
        if (col[0] == '_') {
            indexCol = table->tableName;
            indexCol += '.';
            indexCol += col;
            compOp = gteqConds[i].get<2>();
            value = gteqConds[i].get<1>();
            gteqConds.erase(gteqConds.begin() + i);
            break;
        }
    }

    if (indexCol.empty()) {
        throw std::runtime_error("indexed column not found");
    }
}

IndexScan::IndexScan()
    : indexCol(), compOp(EQ), value(NULL),
      index(NULL), txn(NULL), record(), getNotCalled(), checkIndexCond()
{
}

IndexScan::~IndexScan()
{
}

RC IndexScan::Open(const char *)
{
    file.open(fileName);
    openIndex(indexCol.c_str(), &index);

    record.val.type = value->type;
    record.val.intVal = value->intVal;
    if (value->type == STRING) {
        strcpy(record.val.charVal, value->charVal);
    }

    beginTransaction(&txn);
    getNotCalled = true;
    checkIndexCond = true;

    return 0;
}

RC IndexScan::ReScan(const char *)
{
    commitTransaction(txn);

    record.val.type = value->type;
    record.val.intVal = value->intVal;
    if (value->type == STRING) {
        strcpy(record.val.charVal, value->charVal);
    }

    beginTransaction(&txn);
    getNotCalled = true;
    checkIndexCond = true;

    return 0;
}

RC IndexScan::GetNext(Tuple &tuple)
{
    ErrCode ec;
    Tuple temp;

    if (getNotCalled) {
        ec = get(index, txn, &record);
        getNotCalled = false;

        // GT: the first tuple will be returned in the while loop
        switch (ec) {
        case SUCCESS:
            if (compOp == EQ) {
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
    }

    while ((ec = getNext(index, txn, &record)) == SUCCESS) {
        if (checkIndexCond) {
            int cmp = (value->type == INT) ?
                      (value->intVal - record.val.intVal) :
                      (strcmp(value->charVal, record.val.charVal));

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
    os << "IndexScan@" << getNodeID() << " " << fileName << "    ";
    os << indexCol << ((compOp == EQ) ? " = " : " > ");
    if (value->type == INT) {
        os << value->intVal;
    } else {
        os << value->charVal;
    }
    os << std::endl;
}
