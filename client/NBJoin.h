#ifndef CLIENT_NBJOIN_H_
#define CLIENT_NBJOIN_H_

#include <tr1/unordered_map>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include <boost/smart_ptr/scoped_array.hpp>
#include "client/Join.h"


namespace cardinality {

class NBJoin: public Join {
public:
    NBJoin(const NodeID, Operator::Ptr, Operator::Ptr,
           const Query *);
    explicit NBJoin(google::protobuf::io::CodedInputStream *);
    NBJoin(const NBJoin &);
    ~NBJoin();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

#ifdef PRINT_PLAN
    void print(std::ostream &, const int, const double) const;
#endif

    double estCost(const double = 0.0) const;

protected:
    static uint64_t hashString(const char *, const uint32_t);

    enum { STATE_OPEN, STATE_REOPEN, STATE_GETNEXT, STATE_SWEEPBUFFER } state_;
    bool left_done_;
    typedef std::tr1::unordered_multimap<uint64_t, Tuple> multimap;
    boost::scoped_ptr<multimap> left_tuples_;
    multimap::const_iterator left_tuples_it_;
    multimap::const_iterator left_tuples_end_;
    boost::scoped_array<char> main_buffer_;
    boost::scoped_array<char> overflow_buffer_;

private:
    NBJoin& operator=(const NBJoin &);
};

}  // namespace cardinality

#endif  // CLIENT_NBJOIN_H_
