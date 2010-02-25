#include "SeqScan.h"

using namespace op;


SeqScan::SeqScan(const NodeID n, const char *f, const char *a,
                 const Table *t, const PartitionStats *p, const Query *q)
    : Scan(n, f, a, t, p, q), pos(NULL)
{
}

SeqScan::SeqScan()
    : pos(NULL)
{
}

SeqScan::~SeqScan()
{
}

RC SeqScan::Open(const char *, const uint32_t)
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    file.open(fileName);
    pos = file.begin();
#else
    lineBuffer.reset(new char[(MAX_VARCHAR_LEN + 1) * numInputCols]);
    file.open(fileName.c_str());
    if (file.fail()) {
        throw std::runtime_error("ifstream.open() failed");
    }
#endif
    return 0;
}

RC SeqScan::ReScan(const char *, const uint32_t)
{
#ifndef USE_STD_IFSTREAM_FOR_SCAN
    pos = file.begin();
#else
    file.clear();
    file.seekg(0, std::ios::beg);
#endif
    return 0;
}

RC SeqScan::GetNext(Tuple &tuple)
{
    Tuple temp;

#ifndef USE_STD_IFSTREAM_FOR_SCAN
    while (pos < file.end()) {
        pos = splitLine(pos, file.end(), temp);

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
        file.getline(lineBuffer.get(), (MAX_VARCHAR_LEN + 1) * numInputCols);
        if (*lineBuffer.get() == '\0') {
            return -1;
        }

        temp.clear();

        const char *pos = lineBuffer.get();
        const char *eof = lineBuffer.get() + strlen(lineBuffer.get()) + 1;

        for (int i = 0; i < numInputCols; ++i) {
            const char *delim = static_cast<const char *>(
                                    memchr(pos, (i == numInputCols - 1) ? '\0' : '|', eof - pos));
            temp.push_back(std::make_pair(pos, delim - pos));
            pos = delim + 1;
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
    return 0;
}

void SeqScan::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "SeqScan@" << getNodeID() << " " << fileName;
    os << " #cols=" << numOutputCols();
    os << " len=" << estTupleLength();
    os << " card=" << estCardinality();
    os << " cost=" << estCost();
    os << std::endl;
}

double SeqScan::estCost() const
{
    return 1.0 * ((stats->fileSize + PAGE_SIZE - 1) / PAGE_SIZE);
}

double SeqScan::estCardinality() const
{
    double card = stats->fileSize / stats->tupleLength;

    for (size_t i = 0; i < gteqConds.size(); ++i) {
        if (gteqConds[i].get<2>() == EQ) {
            card /= 10.0;
        } else { // GT
            card /= 3.0;
        }
    }

    if (!joinConds.empty()) {
        card /= 3.0;
    }

    return card;
}
