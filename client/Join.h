#ifndef JOIN_H
#define JOIN_H

#include <boost/shared_ptr.hpp>
#include <boost/tuple/tuple.hpp>
#include "Operator.h"
#include "Scan.h"


namespace op {

class Join: public Operator {
public:
    Join(const Query *, boost::shared_ptr<Operator>, boost::shared_ptr<Scan>);
    virtual ~Join();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

protected:
    void initFilter(const Query *q);
    bool execFilter(Tuple &, Tuple &) const;
    void execProject(Tuple &, Tuple &, Tuple &) const;

    boost::shared_ptr<Operator> leftChild;
    boost::shared_ptr<Scan> rightChild;

    bool reloadLeft;
    Tuple leftTuple;

    std::vector<boost::tuple<ColID, ColID, ValueType> > joinConds;

private:
    Join(const Join &);
    Join& operator=(const Join &);
};

}

#endif
