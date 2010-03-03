#ifndef SEQSCAN_H
#define SEQSCAN_H

#include "Scan.h"


namespace ca {

class SeqScan: public Scan {
public:
    typedef boost::shared_ptr<SeqScan> Ptr;

    SeqScan(const NodeID, const char *, const char *,
            const Table *, const PartitionStats *, const Query *);
    ~SeqScan();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;

    double estCost() const;
    double estCardinality() const;

protected:
    SeqScan();
    SeqScan(const SeqScan &);

    const char *pos_;

private:
    SeqScan& operator=(const SeqScan &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Scan>(*this);
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::SeqScan)

#endif
