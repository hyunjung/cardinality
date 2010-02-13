#ifndef JOIN_H
#define JOIN_H

#include "Operator.h"
#include "Scan.h"


namespace op {

class Join: public Operator {
public:
    typedef boost::shared_ptr<Join> Ptr;

    Join(const NodeID, Operator::Ptr, Scan::Ptr,
         const Query *, const int = -1);
    Join();
    virtual ~Join();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

protected:
    void initFilter(const Query *q, const int);
    bool execFilter(const Tuple &, const Tuple &) const;
    void execProject(const Tuple &, const Tuple &, Tuple &) const;

    Operator::Ptr leftChild;
    Scan::Ptr rightChild;
    std::vector<boost::tuple<ColID, ColID, ValueType> > joinConds;

private:
    Join(const Join &);
    Join& operator=(const Join &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int ver) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & leftChild;
        ar & rightChild;
        ar & joinConds;
    }
};

}

BOOST_SERIALIZATION_ASSUME_ABSTRACT(op::Join)
BOOST_SERIALIZATION_SHARED_PTR(op::Join)

#endif
