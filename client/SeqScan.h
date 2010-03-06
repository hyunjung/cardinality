#ifndef CLIENT_SEQSCAN_H_
#define CLIENT_SEQSCAN_H_

#include "client/Scan.h"


namespace cardinality {

class SeqScan: public Scan {
public:
    typedef boost::shared_ptr<SeqScan> Ptr;

    SeqScan(const NodeID, const char *, const char *,
            const Table *, const PartitionStats *, const Query *);
    ~SeqScan();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

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

}  // namespace cardinality

BOOST_SERIALIZATION_SHARED_PTR(cardinality::SeqScan)

#endif  // CLIENT_SEQSCAN_H_
