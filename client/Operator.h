#ifndef OPERATOR_H
#define OPERATOR_H

#include <vector>
#include <map>
#include <string>
#include <iostream>
#include "../include/client.h"


namespace op {

typedef int RC;
typedef int ColID;
typedef std::vector<const char *> Tuple;


class Operator {
public:
    Operator();
    virtual ~Operator();

    virtual RC open() = 0;
    virtual RC getNext(Tuple &) = 0;
    virtual RC close() = 0;

    virtual void print(std::ostream &, const int = 0) const = 0;
    virtual bool hasCol(const char *) const = 0;
    virtual ColID getInputColID(const char *) const = 0;
    virtual ValueType getColType(const char *) const = 0;

    size_t numOutputCols() const;
    ColID getOutputColID(const char *col) const;

protected:
    void initProject(const Query *q);
    void printOutputCols(std::ostream &) const;

    std::vector<ColID> selectedInputColIDs;
    std::map<std::string, ColID> outputColToColID;

private:
    Operator(const Operator &);
    Operator& operator=(const Operator &);
};

}

#endif
