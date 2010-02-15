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

RC SeqScan::Open(const char *)
{
    lineBuffer.reset(new char[(MAX_VARCHAR_LEN + 1) * numInputCols]);

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

RC SeqScan::ReScan(const char *)
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    pos = file.begin();
#else
    file.seekg(0, std::ios::beg);
#endif
    return 0;
}

RC SeqScan::GetNext(Tuple &tuple)
{
    Tuple temp;

#ifndef USE_STD_IFSTREAM_FOR_SCAN
    while (pos < file.end()) {
        pos = splitLine(pos, file.end(),
                        lineBuffer.get(), temp);

        if (execFilter(temp)) {
            execProject(temp, tuple);
            return 0;
        }
    }

    return -1;
#else
    while (true) {
        if (file.eof()) {
            return -1;
        }
        file.getline(lineBuffer.get(), 4096);
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
#endif
}

RC SeqScan::Close()
{
    file.close();
    lineBuffer.reset();

    return 0;
}

void SeqScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan@" << getNodeID() << " " << fileName << "    ";
    printOutputCols(os);
    os << std::endl;
}
