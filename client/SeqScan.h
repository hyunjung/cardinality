#ifndef SEQSCAN_H
#define SEQSCAN_H

#include "Scan.h"


namespace op {

class SeqScan: public Scan {
public:
    SeqScan(const NodeID, const char *, const char *, const Table *, const Query *);
    SeqScan();
    ~SeqScan();

    RC open();
    RC getNext(Tuple &);
    RC close();

    void print(std::ostream &, const int) const;

protected:
    const char *pos;

private:
    SeqScan(const SeqScan &);
    SeqScan& operator=(const SeqScan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Scan>(*this);
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(op::SeqScan)

#endif
