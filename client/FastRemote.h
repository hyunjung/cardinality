#ifndef CLIENT_FASTREMOTE_H_
#define CLIENT_FASTREMOTE_H_

#include "client/Remote.h"


namespace cardinality {

class FastRemote: public Remote {
public:
    FastRemote(const NodeID, Operator::Ptr,
               const boost::asio::ip::address_v4 &);
    ~FastRemote();
    Operator::Ptr clone() const;

    void Open(const char * = NULL, const uint32_t = 0);

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;

private:
    FastRemote& operator=(const FastRemote &);
};

}  // namespace cardinality

#endif  // CLIENT_FASTREMOTE_H_
