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
    explicit Remote(google::protobuf::io::CodedInputStream *);
    Remote(const Remote &);
    ~Remote();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    std::pair<const PartStats *, ColID> getPartStats(const ColID) const;
    ValueType getColType(const char *) const;
    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

    double estCost(const double = 0.0) const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    boost::asio::ip::address_v4 ip_address_;
    Operator::Ptr child_;

    tcpsocket_ptr socket_;
    boost::scoped_ptr<boost::asio::streambuf> buffer_;

    static const double COST_NET_XFER_BYTE = 0.0025;

private:
    Remote& operator=(const Remote &);
};

}  // namespace cardinality

#endif  // CLIENT_REMOTE_H_
