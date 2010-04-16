#ifndef CLIENT_REMOTE_H_
#define CLIENT_REMOTE_H_

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include "client/Operator.h"


namespace cardinality {

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;

class Remote: public Operator {
public:
    Remote(const NodeID, Operator::Ptr,
           const boost::asio::ip::address_v4 &);
    Remote();
    Remote(const Remote &);
    ~Remote();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    void Serialize(google::protobuf::io::CodedOutputStream *) const;
    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ColID getBaseColID(const ColID) const;
    const PartitionStats *getPartitionStats(const char *) const;
    ValueType getColType(const char *) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

    double estCost(const double = 0.0) const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Operator::Ptr child_;
    boost::asio::ip::address_v4 ip_address_;

    tcpsocket_ptr socket_;
    bool socket_reusable_;
    boost::scoped_ptr<boost::asio::streambuf> response_;

private:
    Remote& operator=(const Remote &);
};

}  // namespace cardinality

#endif  // CLIENT_REMOTE_H_
