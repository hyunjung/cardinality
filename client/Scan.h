#ifndef SCAN_H
#define SCAN_H

#include <fstream>
#include <boost/tuple/tuple.hpp>
#include <boost/shared_array.hpp>
#include "Operator.h"


namespace op {

class Scan: public Operator {
public:
    Scan(const Query *, const char *, const Table *, const char *);
    virtual ~Scan();

    bool hasCol(const char *) const;
    ColID getInputColID(const char *) const;
    ValueType getColType(const char *) const;

protected:
    void initFilter(const Query *q);
    bool execFilter(Tuple &) const;
    void execProject(Tuple &, Tuple &) const;

    const char *alias;
    const int lenAlias;
    const Table *table;
    const char *fileName;
    std::ifstream ifs;
    boost::shared_array<char> lineBuffer;

    std::vector<std::pair<ColID, Value *> > eqConds;
    std::vector<std::pair<ColID, Value *> > gtConds;
    std::vector<boost::tuple<ColID, ColID, ValueType> > joinConds;

private:
    Scan(const Scan &);
    Scan& operator=(const Scan &);
};

}

#endif
