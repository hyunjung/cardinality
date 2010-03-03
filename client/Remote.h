#ifndef REMOTE_H
#define REMOTE_H

#include <boost/asio/ip/tcp.hpp>
#include <boost/scoped_array.hpp>
#include "Operator.h"


namespace ca {

class Remote: public Operator {
public:
    typedef boost::shared_ptr<Remote> Ptr;

    Remote(const NodeID, Operator::Ptr, const char *);
    ~Remote();
    Operator::Ptr clone() const;

    RC Open(const char * = NULL, const uint32_t = 0);
    RC ReOpen(const char * = NULL, const uint32_t = 0);
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

    double estCost() const;
    double estCardinality() const;
    double estTupleLength() const;
    double estColLength(const ColID) const;

protected:
    Remote();
    Remote(const Remote &);

    Operator::Ptr child_;
    const std::string hostname_;

    boost::asio::ip::tcp::iostream tcpstream_;
    boost::scoped_array<char> line_buffer_;

private:
    Remote& operator=(const Remote &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & child_;
        ar & const_cast<std::string &>(hostname_);
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(ca::Remote)

#endif
