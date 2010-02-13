#include "NBJoin.h"

#ifndef NBJOIN_BUFSIZE
#define NBJOIN_BUFSIZE 65536
#endif

using namespace op;


NBJoin::NBJoin(const NodeID n, Operator::Ptr l, Scan::Ptr r,
               const Query *q)
    : Join(n, l, r, q),
      state(), leftDone(), leftTuples(), leftTuplesIt(), rightTuple(),
      mainBuffer(new char[NBJOIN_BUFSIZE]), overflowBuffer()
{
}

NBJoin::NBJoin()
    : state(), leftDone(), leftTuples(), leftTuplesIt(), rightTuple(),
      mainBuffer(new char[NBJOIN_BUFSIZE]), overflowBuffer()
{
}

NBJoin::~NBJoin()
{
}

RC NBJoin::Open(const char *)
{
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
                    int len = strlen(leftTuple[i]);
                    if (pos + len < mainBuffer.get() + NBJOIN_BUFSIZE) {
                        memcpy(pos, leftTuple[i], len);
                        pos[len] = '\0';
                        leftTuple[i] = pos;
                        pos += len + 1;
                    } else { // mainBuffer doesn't have enough space
                        int overflowLen = len + 1;
                        for (size_t j = i + 1; j < leftTuple.size(); ++j) {
                            overflowLen += strlen(leftTuple[j]) + 1;
                        }
                        overflowBuffer.reset(new char[overflowLen]);
                        pos = overflowBuffer.get();
                        for (; i < leftTuple.size(); ++i) {
                            int len = strlen(leftTuple[i]);
                            memcpy(pos, leftTuple[i], len);
                            pos[len] = '\0';
                            leftTuple[i] = pos;
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
    return leftChild->Close();
}

void NBJoin::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "NBJoin@" << getNodeID() << "    ";
    printOutputCols(os);
    os << std::endl;

    leftChild->print(os, tab + 1);
    rightChild->print(os, tab + 1);
}
