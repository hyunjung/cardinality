#include "client/Dummy.h"


namespace cardinality {

Dummy::Dummy(const NodeID n)
    : Operator(n)
{
}

Dummy::Dummy()
    : Operator()
{
}

Dummy::Dummy(const Dummy &x)
    : Operator(x)
{
}

Dummy::~Dummy()
{
}

Operator::Ptr Dummy::clone() const
{
    return boost::make_shared<Dummy>(*this);
}

void Dummy::Open(const char *left_ptr, const uint32_t left_len)
{
}

void Dummy::ReOpen(const char *left_ptr, const uint32_t left_len)
{
}

bool Dummy::GetNext(Tuple &tuple)
{
    return true;
}

void Dummy::Close()
{
}

void Dummy::Serialize(google::protobuf::io::CodedOutputStream *output) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

int Dummy::ByteSize() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

void Dummy::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

void Dummy::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Dummy@" << node_id();
    os << std::endl;
}

bool Dummy::hasCol(const char *col) const
{
    return false;
}

ColID Dummy::getInputColID(const char *col) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

ColID Dummy::getBaseColID(const ColID) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

const PartitionStats *Dummy::getPartitionStats(const char *) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

ValueType Dummy::getColType(const char *col) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

ColID Dummy::numOutputCols() const
{
    return 0;
}

ColID Dummy::getOutputColID(const char *col) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

double Dummy::estCost(const double) const
{
    return 0.0;
}

double Dummy::estCardinality() const
{
    return 0.0;
}

double Dummy::estTupleLength() const
{
    return 0.0;
}

double Dummy::estColLength(const ColID cid) const
{
    return 0.0;
}

}  // namespace cardinality
