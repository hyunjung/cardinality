#include "Operator.h"


struct Connection
{
    Connection(): q(NULL), root(), tuple() {};
    ~Connection() {};

    const Query *q;
    op::Operator::Ptr root;
    op::Tuple tuple;

private:
    Connection(const Connection &);
    Connection& operator=(const Connection &);
};
