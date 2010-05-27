#include "client/Material.h"
#include <cstring>
#include <stdexcept>  // std::runtime_error


namespace cardinality {

static const int MATERIAL_BUFSIZE = 2097152;  // 2 MB

Material::Material(const NodeID n, Operator::Ptr c)
    : Operator(n),
      child_(c),
      tuples_(new std::vector<Tuple>()), tuples_it_(),
      buffer_(new char[MATERIAL_BUFSIZE])
{
    Tuple tuple;
    char *eob = buffer_.get();

    c->Open();
    while (!c->GetNext(tuple)) {
        for (int i = 0; i < tuple.size(); ++i) {
            if (eob + tuple[i].second - buffer_.get() > MATERIAL_BUFSIZE) {
                c->Close();
                throw std::runtime_error("result too large");
            }

            std::memcpy(eob, tuple[i].first, tuple[i].second);
            tuple[i].first = eob;
            eob += tuple[i].second;
        }

        tuples_->push_back(tuple);
    }
    c->Close();
}

Material::Material(const Material &x)
    : Operator(x),
      child_(x.child_),
      tuples_(x.tuples_), tuples_it_(),
      buffer_(x.buffer_)
{
}

Material::~Material()
{
}

Operator::Ptr Material::clone() const
{
    return boost::make_shared<Material>(*this);
}

void Material::Open(const char *, const uint32_t)
{
    tuples_it_ = tuples_->begin();
}

void Material::ReOpen(const char *, const uint32_t)
{
    tuples_it_ = tuples_->begin();
}

bool Material::GetNext(Tuple &tuple)
{
    if (tuples_it_ == tuples_->end()) {
        return true;
    }

    tuple = *tuples_it_++;
    return false;
}

void Material::Close()
{
}

uint8_t *Material::SerializeToArray(uint8_t *target) const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

int Material::ByteSize() const
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

void Material::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    throw std::runtime_error(BOOST_CURRENT_FUNCTION);
}

#ifdef PRINT_PLAN
void Material::print(std::ostream &os, const int tab, const double) const
{
    os << std::string(4 * tab, ' ');
    os << "Material@" << node_id();
    os << " cost=" << estCost();
    os << std::endl;

    child_->print(os, tab + 1);
}
#endif

bool Material::hasCol(const char *col) const
{
    return child_->hasCol(col);
}

ColID Material::getInputColID(const char *col) const
{
    return child_->getOutputColID(col);
}

std::pair<const PartStats *, ColID> Material::getPartStats(const ColID cid) const
{
    return child_->getPartStats(cid);
}

ValueType Material::getColType(const char *col) const
{
    return child_->getColType(col);
}

ColID Material::numOutputCols() const
{
    return child_->numOutputCols();
}

ColID Material::getOutputColID(const char *col) const
{
    return child_->getOutputColID(col);
}

double Material::estCost(const double) const
{
    return 0.0;
}

double Material::estCardinality(const bool) const
{
    return static_cast<double>(tuples_->size());
}

double Material::estTupleLength() const
{
    return child_->estTupleLength();
}

double Material::estColLength(const ColID cid) const
{
    return child_->estColLength(cid);
}

}  // namespace cardinality
