#include "SeqScan.h"

using namespace ca;


SeqScan::SeqScan(const NodeID n, const char *f, const char *a,
                 const Table *t, const PartitionStats *p, const Query *q)
    : Scan(n, f, a, t, p, q),
      pos()
{
}

SeqScan::SeqScan()
    : Scan(),
      pos()
{
}

SeqScan::SeqScan(const SeqScan &x)
    : Scan(x),
      pos()
{
}

SeqScan::~SeqScan()
{
}

Operator::Ptr SeqScan::clone() const
{
    return Operator::Ptr(new SeqScan(*this));
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

RC SeqScan::ReOpen(const char *, const uint32_t)
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
        const char *eof = lineBuffer.get() + std::strlen(lineBuffer.get()) + 1;

        for (int i = 0; i < numInputCols; ++i) {
            const char *delim = static_cast<const char *>(
                                    std::memchr(pos, (i == numInputCols - 1) ? '\0' : '|', eof - pos));
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
    return stats->numPages * COST_DISK_READ_PAGE;
}

double SeqScan::estCardinality() const
{
    double card = stats->cardinality;

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
