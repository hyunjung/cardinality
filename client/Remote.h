#ifndef REMOTE_H
#define REMOTE_H

#include <boost/asio/ip/tcp.hpp>
#include <boost/shared_array.hpp>
#include "Operator.h"


namespace op {

class Remote: public Operator {
public:
    Remote(const NodeID, boost::shared_ptr<Operator>, const char *);
    Remote();
    ~Remote();

    RC Open();
    RC GetNext(Tuple &);
    RC Close();

    void print(std::ostream &, const int) const;
    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

protected:
    boost::shared_ptr<Operator> child;
    const std::string ipAddress;

    boost::asio::ip::tcp::iostream tcpstream;
    boost::shared_array<char> lineBuffer;

private:
    Remote(const Remote &);
    Remote& operator=(const Remote &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & child;
        ar & const_cast<std::string &>(ipAddress);
    }
};

}

BOOST_SERIALIZATION_SHARED_PTR(op::Remote)

#endif
