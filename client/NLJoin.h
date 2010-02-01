#ifndef NLJOIN_H
#define NLJOIN_H

#include "Join.h"


namespace op {

class NLJoin: public Join {
public:
    NLJoin(const Query *, boost::shared_ptr<Operator>, boost::shared_ptr<Scan>);
    ~NLJoin();

    RC open();
    RC getNext(Tuple &);
    RC close();

    void print(std::ostream &, const int) const;

private:
    NLJoin(const NLJoin &);
    NLJoin& operator=(const NLJoin &);
};

}

#endif
