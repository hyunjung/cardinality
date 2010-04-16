#include "client/Union.h"


namespace cardinality {

Union::Union(const NodeID n, std::vector<Operator::Ptr> c)
    : Operator(n),
      children_(c), it_(), done_()
{
}

Union::Union()
    : Operator(),
      children_(), it_(), done_()
{
}

Union::Union(const Union &x)
    : Operator(x),
      children_(), it_(), done_()
{
    children_.reserve(x.children_.size());
    for (std::vector<Operator::Ptr>::const_iterator it = x.children_.begin();
         it < x.children_.end(); ++it) {
        children_.push_back((*it)->clone());
    }
}

Union::~Union()
{
}

Operator::Ptr Union::clone() const
{
    return boost::make_shared<Union>(*this);
}

void Union::Open(const char *left_ptr, const uint32_t left_len)
{
    done_.resize(children_.size());
    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->Open(left_ptr, left_len);
        done_[i] = false;
    }
    it_ = 0;
}

void Union::ReOpen(const char *left_ptr, const uint32_t left_len)
{
    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->ReOpen(left_ptr, left_len);
        done_[i] = false;
    }
    it_ = 0;
}

bool Union::GetNext(Tuple &tuple)
{
    for (; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return false;
            }
        }
    }

    for (it_ = 0; it_ < children_.size(); ++it_) {
        if (!done_[it_]) {
            if (children_[it_]->GetNext(tuple)) {
                done_[it_] = true;
            } else {
                ++it_;
                return false;
            }
        }
    }

    return true;
}

void Union::Close()
{
    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->Close();
    }
}

void Union::Serialize(google::protobuf::io::CodedOutputStream *output) const
{
    output->WriteVarint32(6);

    output->WriteVarint32(node_id_);

    output->WriteVarint32(children_.size());
    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->Serialize(output);
    }
}

uint8_t *Union::SerializeToArray(uint8_t *target) const
{
    using google::protobuf::io::CodedOutputStream;

    target = CodedOutputStream::WriteVarint32ToArray(6, target);

    target = CodedOutputStream::WriteVarint32ToArray(node_id_, target);

    target = CodedOutputStream::WriteVarint32ToArray(children_.size(), target);
    for (std::size_t i = 0; i < children_.size(); ++i) {
        target = children_[i]->SerializeToArray(target);
    }

    return target;
}

int Union::ByteSize() const
{
    using google::protobuf::io::CodedOutputStream;

    int total_size = 1;

    total_size += CodedOutputStream::VarintSize32(node_id_);

    total_size += CodedOutputStream::VarintSize32(children_.size());
    for (std::size_t i = 0; i < children_.size(); ++i) {
        total_size += children_[i]->ByteSize();
    }

    return total_size;
}

void Union::Deserialize(google::protobuf::io::CodedInputStream *input)
{
    input->ReadVarint32(&node_id_);

    uint32_t size;
    input->ReadVarint32(&size);
    children_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        children_.push_back(parsePlan(input));
    }
}

void Union::print(std::ostream &os, const int tab) const
{
    os << std::string(4 * tab, ' ');
    os << "Union@" << node_id();
    os << " cost=" << estCost();
    os << std::endl;

    for (std::size_t i = 0; i < children_.size(); ++i) {
        children_[i]->print(os, tab + 1);
    }
}

bool Union::hasCol(const char *col) const
{
    return children_[0]->hasCol(col);
}

ColID Union::getInputColID(const char *col) const
{
    return children_[0]->getOutputColID(col);
}

ColID Union::getBaseColID(const ColID cid) const
{
    return children_[0]->getBaseColID(cid);
}

const PartitionStats *Union::getPartitionStats(const char *) const
{
    return NULL;
}

ValueType Union::getColType(const char *col) const
{
    return children_[0]->getColType(col);
}

ColID Union::numOutputCols() const
{
    return children_[0]->numOutputCols();
}

ColID Union::getOutputColID(const char *col) const
{
    return children_[0]->getOutputColID(col);
}

double Union::estCost(const double left_cardinality) const
{
    double cost = 0.0;
    for (std::size_t i = 0; i < children_.size(); ++i) {
        cost += children_[i]->estCost(left_cardinality);
    }

    return cost;
}

double Union::estCardinality() const
{
    double card = 0.0;
    for (std::size_t i = 0; i < children_.size(); ++i) {
        card += children_[i]->estCardinality();
    }

    return card;
}

double Union::estTupleLength() const
{
    return children_[0]->estTupleLength();
}

double Union::estColLength(const ColID cid) const
{
    return children_[0]->estColLength(cid);
}

}  // namespace cardinality
