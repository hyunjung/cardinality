#ifndef CLIENT_PROJECT_H_
#define CLIENT_PROJECT_H_

#include "client/Operator.h"


namespace cardinality {

class Project: public Operator {
public:
    explicit Project(const NodeID);
    Project(const Project &);
    virtual ~Project();

    ColID numOutputCols() const;
    ColID getOutputColID(const char *) const;

protected:
    Project();

    void initProject(const Query *);

    std::vector<ColID> selected_input_col_ids_;

private:
    Project& operator=(const Project &);

    friend class boost::serialization::access;
    template<class Archive> void serialize(Archive &ar, const unsigned int) {
        ar & boost::serialization::base_object<Operator>(*this);
        ar & selected_input_col_ids_;
    }
};

}  // namespace cardinality

BOOST_SERIALIZATION_ASSUME_ABSTRACT(cardinality::Project)

#endif  // CLIENT_PROJECT_H_
