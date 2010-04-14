#ifndef CLIENT_PROJECT_H_
#define CLIENT_PROJECT_H_

#include "client/Operator.h"


namespace cardinality {

class Project: public Operator {
public:
    explicit Project(const NodeID);
    Project();
    Project(const Project &);
    virtual ~Project();

    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

protected:
    void initProject(const Query *);

    std::vector<ColID> selected_input_col_ids_;

private:
    Project& operator=(const Project &);
};

}  // namespace cardinality

#endif  // CLIENT_PROJECT_H_
