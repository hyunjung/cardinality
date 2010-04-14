#include "client/Operator.h"
#include <boost/spirit/include/qi.hpp>
#include "client/SeqScan.h"
#include "client/IndexScan.h"
#include "client/NLJoin.h"
#include "client/NBJoin.h"
#include "client/Remote.h"
#include "client/Union.h"


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

Operator::Ptr Operator::parsePlan(google::protobuf::io::CodedInputStream *input)
{
    uint32_t operator_type;
    input->ReadVarint32(&operator_type);

    Operator::Ptr plan;

    switch (operator_type) {
    case 1:  // SeqScan
        plan = boost::make_shared<SeqScan>();
        break;

    case 2:  // IndexScan
        plan = boost::make_shared<IndexScan>();
        break;

    case 3:  // NLJoin
        plan = boost::make_shared<NLJoin>();
        break;

    case 4:  // NBJoin
        plan = boost::make_shared<NBJoin>();
        break;

    case 5:  // Remote
        plan = boost::make_shared<Remote>();
        break;

    case 6:  // Union
        plan = boost::make_shared<Union>();
        break;
    }

    plan->Deserialize(input);

    return plan;
}

}  // namespace cardinality
