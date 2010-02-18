#include "NBJoin.h"

#ifndef NBJOIN_BUFSIZE
#define NBJOIN_BUFSIZE 65536
#endif

using namespace op;


NBJoin::NBJoin(const NodeID n, Operator::Ptr l, Scan::Ptr r,
               const Query *q)
    : Join(n, l, r, q),
      state(), leftDone(), leftTuples(), leftTuplesIt(), rightTuple(),
      mainBuffer(), overflowBuffer()
{
}

NBJoin::NBJoin()
    : state(), leftDone(), leftTuples(), leftTuplesIt(), rightTuple(),
      mainBuffer(), overflowBuffer()
{
}

NBJoin::~NBJoin()
{
}

RC NBJoin::Open(const char *, const uint32_t)
{
    mainBuffer.reset(new char[NBJOIN_BUFSIZE]);
    state = RIGHT_OPEN;
    return leftChild->Open();
}

RC NBJoin::GetNext(Tuple &tuple)
{
    while (true) {
        switch (state) {
        case RIGHT_OPEN:
        case RIGHT_RESCAN: {
            Tuple leftTuple;
            for (char *pos = mainBuffer.get();
                 pos - mainBuffer.get() < NBJOIN_BUFSIZE - 512
                 && !(leftDone = leftChild->GetNext(leftTuple)); ) {
                for (size_t i = 0; i < leftTuple.size(); ++i) {
                    uint32_t len = leftTuple[i].second;
                    if (pos + len < mainBuffer.get() + NBJOIN_BUFSIZE) {
                        memcpy(pos, leftTuple[i].first, len);
                        pos[len] = '\0';
                        leftTuple[i].first = pos;
                        pos += len + 1;
                    } else { // mainBuffer doesn't have enough space
                        int overflowLen = len + 1;
                        for (size_t j = i + 1; j < leftTuple.size(); ++j) {
                            overflowLen += leftTuple[j].second + 1;
                        }
                        overflowBuffer.reset(new char[overflowLen]);
                        pos = overflowBuffer.get();
                        for (; i < leftTuple.size(); ++i) {
                            uint32_t len = leftTuple[i].second;
                            memcpy(pos, leftTuple[i].first, len);
                            pos[len] = '\0';
                            leftTuple[i].first = pos;
                            pos += len + 1;
                        }
                        pos = mainBuffer.get() + NBJOIN_BUFSIZE;
                    }
                }
                leftTuples.push_back(leftTuple);
            }

            if (leftTuples.empty()) {
                if (state == RIGHT_RESCAN) {
                    rightChild->Close();
                }
                return -1;
            }

            if (state == RIGHT_OPEN) {
                rightChild->Open();
            } else { // RIGHT_RESCAN
                rightChild->ReScan();
            }
            state = RIGHT_GETNEXT;
        }

        case RIGHT_GETNEXT:
            if (rightChild->GetNext(rightTuple)) {
                leftTuples.clear();
                if (leftDone) {
                    rightChild->Close();
                    return -1;
                }
                state = RIGHT_RESCAN;
                break;
            }
            leftTuplesIt = leftTuples.begin();
            state = RIGHT_SWEEPBUFFER;

        case RIGHT_SWEEPBUFFER:
            for (; leftTuplesIt < leftTuples.end(); ++leftTuplesIt) {
                if (execFilter(*leftTuplesIt, rightTuple)) {
                    execProject(*leftTuplesIt++, rightTuple, tuple);
                    return 0;
                }
            }
            state = RIGHT_GETNEXT;
            break;
        }
    }

    return 0;
}

RC NBJoin::Close()
{
    mainBuffer.reset();
    overflowBuffer.reset();
    return leftChild->Close();
}

void NBJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NBJoin@" << getNodeID();
    os << " [" << selectedInputColIDs.size() << "] ";
    os << std::endl;

    leftChild->print(os, tab + 1);
    rightChild->print(os, tab + 1);
}
