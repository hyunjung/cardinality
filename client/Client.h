#include <map>
#include <boost/shared_ptr.hpp>
#include "../include/client.h"
#include "Operator.h"


static const Nodes * sNodes ;
static Ports ports ;
static const Data * sData ;
static std::map<std::string, Table * > sTables ;

struct Connection
{
    Connection(): q(NULL), root(), tuple() {};
    ~Connection() {};
    const Query *q;
    boost::shared_ptr<op::Operator> root;
    op::Tuple tuple;
private:
    Connection(const Connection &);
    Connection& operator=(const Connection &);
};
