#ifndef CLIENT_REMOTE_H_
#define CLIENT_REMOTE_H_

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>
#include "client/Operator.h"


namespace cardinality {

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> tcpsocket_ptr;

class Remote: public Operator {
public:
    Remote(const NodeID, Operator::Ptr, const char *);
    ~Remote();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);
    void ReOpen(const char * = NULL, const uint32_t = 0);
    bool GetNext(Tuple &);
    void Close();

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
    Remote();
    Remote(const Remote &);

    Operator::Ptr child_;
    const std::string ip_address_;

    tcpsocket_ptr socket_;
    bool socket_reusable_;
    boost::asio::streambuf response_;

private:
    Remote& operator=(const Remote &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & child_;
        ar & const_cast<std::string &>(ip_address_);
    }
};

}  // namespace cardinality

#endif  // CLIENT_REMOTE_H_
