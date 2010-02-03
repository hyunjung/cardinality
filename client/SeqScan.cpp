#include <cstring>
#include "SeqScan.h"

using namespace op;


SeqScan::SeqScan(const NodeID n, const char *f, const char *a, const Table *t, const Query *q)
    : Scan(n, f, a, t, q)
{
}

SeqScan::SeqScan()
{
}

SeqScan::~SeqScan()
{
}

RC SeqScan::open()
{
    ifs.open(fileName.c_str());
    if (ifs.fail()) {
        throw std::runtime_error("ifstream.open() failed");
    }
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
        while (true) {
            temp.push_back(c);
            if (!(c = strchr(c + 1, '|'))) {
                break;
            }
            *c++ = 0;
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
    os << "SeqScan@" << getNodeID() << " " << fileName << "    ";
    printOutputCols(os);
    os << std::endl;
}
