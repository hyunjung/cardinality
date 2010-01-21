
#ifndef __CLIENT_HELPER__SIGMOD__10__
#define __CLIENT_HELPER__SIGMOD__10__

#include "../include/client.h"
#include "../lib/Tools.h"
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <vector>

using namespace std ;

Node createNode(const char * ip, int iNode);
Nodes createNodes(vector<Node> & cppNode );
Partition createPartition(int iNode, int nbFields, string filename, Value vinf, Value vsup);
Table createTable(string tableName, vector<string> & fieldsName, vector<ValueType> & fieldsType, vector<Partition> & partitions );
Data createData(vector<Table> & tables);

void deleteNodes(Nodes & nodes);
void deletePartition(Partition & partition);
void deleteTable(Table & table);
void deleteData(Data & data);
void deleteQuery(Query & query);

ValueType getTypeFromString(string s);
Value getValueFromString(string s);
void createCopyTrim(const string & str, char* & out );


Data createDataFromDirectory(string directory, vector<Node> & nodes, int & nbSeconds );
void createQueryFromDirectory(string directory, vector<Query> & preset ,vector<Query> & queries, uint64_t & nbRowsExpected, uint64_t & hashExpected );

void createQueryFromString(Query & query, string & line);
Queries createQueries(vector<Query> & queries);

#endif

