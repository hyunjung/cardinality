#include <cstring>
#include "SeqScan.h"

using namespace op;


SeqScan::SeqScan(const NodeID n, const char *f, const char *a, const Table *t, const Query *q)
    : Scan(n, f, a, t, q), pos(NULL)
{
}

SeqScan::SeqScan()
    : pos(NULL)
{
}

SeqScan::~SeqScan()
{
}

RC SeqScan::open()
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    file.open(fileName);
    pos = file.begin();
#else
    file.open(fileName.c_str());
    if (file.fail()) {
        throw std::runtime_error("ifstream.open() failed");
    }
#endif
    return 0;
}

RC SeqScan::getNext(Tuple &tuple)
{
    Tuple temp;

#ifndef USE_STD_IFSTREAM_FOR_SCAN
    while (pos < file.end()) {
        const char *eol = strchr(pos, '\n');
        if (pos == eol) {
            return -1;
        }
        memcpy(lineBuffer.get(), pos, eol - pos);
        lineBuffer.get()[eol - pos] = '\0';
        pos = eol + 1;
#else
    while (true) {
        if (file.eof()) {
            return -1;
        }
        file.getline(lineBuffer.get(), 4096);
        if (*lineBuffer.get() == '\0') {
            return -1;
        }
#endif

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

    return -1;
}

RC SeqScan::close()
{
    file.close();
    return 0;
}

void SeqScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan@" << getNodeID() << " " << fileName << "    ";
    printOutputCols(os);
    os << std::endl;
}
