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
    Remote(const Remote &);
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

    Operator::Ptr child_;
    const boost::asio::ip::address_v4 ip_address_;

    tcpsocket_ptr socket_;
    bool socket_reusable_;
    boost::scoped_ptr<boost::asio::streambuf> response_;

private:
    Remote& operator=(const Remote &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & child_;
        ar & const_cast<boost::asio::ip::address_v4 &>(ip_address_);
    }
};

}  // namespace cardinality

BOOST_CLASS_IMPLEMENTATION(cardinality::Remote,
                           boost::serialization::object_serializable)
BOOST_CLASS_TRACKING(cardinality::Remote,
                     boost::serialization::track_never)

#include <boost/serialization/split_free.hpp>

namespace boost {
namespace serialization {

template<class Archive>
void save(Archive &ar, const boost::asio::ip::address_v4 &a, const unsigned int)
{
    unsigned long addr = a.to_ulong();
    ar << addr;
}

template<class Archive>
void load(Archive &ar, boost::asio::ip::address_v4 &a, const unsigned int)
{
    unsigned long addr;
    ar >> addr;
    a = boost::asio::ip::address_v4(addr);
}
}}

BOOST_SERIALIZATION_SPLIT_FREE(boost::asio::ip::address_v4)

BOOST_CLASS_IMPLEMENTATION(boost::asio::ip::address_v4,
                           boost::serialization::object_serializable)
BOOST_CLASS_TRACKING(boost::asio::ip::address_v4,
                     boost::serialization::track_never)

#endif  // CLIENT_REMOTE_H_
