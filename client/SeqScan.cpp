#include <cstring>
#include "SeqScan.h"

using namespace op;


SeqScan::SeqScan(const Query *q, const char *_alias, const Table *_table, const char *_fileName)
    : Scan(q, _alias, _table, _fileName)
{
}

SeqScan::~SeqScan()
{
}

RC SeqScan::open()
{
    ifs.open(fileName);
    return 0;
}

RC SeqScan::getNext(Tuple &tuple)
{
    Tuple temp;

    while (true) {
        if (ifs.eof()) {
            return -1;
        }
        ifs.getline(lineBuffer.get(), 4096);
        if (*lineBuffer.get() == '\0') {
            return -1;
        }

        char *c = lineBuffer.get();
        temp.clear();
        for (int i = 0; i < table->nbFields; ++i) {
            temp.push_back(c);
            if ((c = strchr(c + 1, '|'))) {
                *c++ = 0;
            }
        }

        if (execFilter(temp)) {
            execProject(temp, tuple);
            return 0;
        }
    }
}

RC SeqScan::close()
{
    ifs.close();
    return 0;
}

void SeqScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan ";
    os << table->tableName << " as " << alias << "    ";
    printOutputCols(os);
    os << std::endl;
}
