#include "client/Operator.h"
#include <boost/spirit/include/qi.hpp>


namespace cardinality {

Operator::Operator(const NodeID n)
    : node_id_(n)
{
}

Operator::Operator()
    : node_id_()
{
}

Operator::Operator(const Operator &x)
    : node_id_(x.node_id_)
{
}

Operator::~Operator()
{
}

NodeID Operator::node_id() const
{
    return node_id_;
}

uint32_t Operator::parseInt(const char *str, const uint32_t len)
{
    uint32_t intval = 0;
    boost::spirit::qi::parse(str, str + len, boost::spirit::uint_, intval);
    return intval;
}

}  // namespace cardinality
