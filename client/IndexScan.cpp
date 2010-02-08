#include "IndexScan.h"

using namespace op;


IndexScan::IndexScan(const Query *q, const char *_alias, const Table *_table, const char *_fileName)
    : Scan(q, _alias, _table, _fileName)
{
}

IndexScan::~IndexScan()
{
}

RC IndexScan::Open()
{
    ifs.open(fileName);
    return (ifs.is_open()) ? 0 : -1;
}

RC IndexScan::GetNext(Tuple &tuple)
{
    std::string lineBuffer;
    Tuple temp;

    while (true) {
        if (ifs.eof()) {
            return -1;
        }
        std::getline(ifs, lineBuffer);
        if (lineBuffer.size() == 0) {
            return -1;
        }

        boost::split(temp, lineBuffer, boost::is_any_of("|\n"));

        if (execFilter(temp)) {
            execProject(temp, tuple);
            return 0;
        }
    }
}

RC IndexScan::close()
{
    ifs.close();
    return 0;
}

void IndexScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "IndexScan ";
    os << table->tableName << " as " << alias << "    ";
    printOutputCols(os);
    os << std::endl;
}
