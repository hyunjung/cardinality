#ifndef CLIENT_PROJECT_H_
#define CLIENT_PROJECT_H_

#include <vector>
#include "client/Operator.h"


namespace cardinality {

class Project: public Operator {
public:
    explicit Project(const NodeID);
    explicit Project(google::protobuf::io::CodedInputStream *);
    Project(const Project &);
    ~Project();

    uint8_t *SerializeToArray(uint8_t *) const;
    int ByteSize() const;
    void Deserialize(google::protobuf::io::CodedInputStream *);

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
