
#ifndef __MASTER__SIGMOD__10__
#define __MASTER__SIGMOD__10__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************
 *
 * Data structures
 *
 ******************************************/

const int MAX_VARCHAR_LEN = 1000 ; 

// Prototypes
struct Partition ;
struct Node ;
struct Index ;

typedef enum ErrCode
{
  SUCCESS,
  DB_DNE,
  DB_EXISTS,
  DB_END,
  KEY_NOTFOUND,
  TXN_EXISTS,
  TXN_DNE,
  ENTRY_EXISTS,
  ENTRY_DNE,
  DEADLOCK,
  FAILURE
} ErrCode;

/**
 * There are two types: INT or STRING
 * INT are 4 bytes int
 * STRING are strings with 1000 max characters ( '\0' excluded )
 *
 * These two types are used for keys and values.
 * There is no other type of data in the database.
 * The '\0' will be stored at the end of a string in the database.
 * The actual size in bytes of a string can be up to 1001.
 */
enum ValueType {
  INT ,
  STRING
} ;

struct Value {
  ValueType type;
  uint32_t intVal;
  char charVal[MAX_VARCHAR_LEN + 1];
} ;

struct Record {
  Value val;
  uint64_t address;
} ;


/**
 * This structures contains the query.
 *
 * sqlQuery is string representing the query. The following fields
 * are a representation of this SQL query. This representation is guaranteed 
 * to be correct.
 *
 * The names of the fields are always specified as alias_name.column_name
 *
 * You are guaranteed that all the table names are different, and all alias
 * names are different. However the same table can have multiple aliases:
 * self-join are possible. An alias name can be equal to a table name.
 */

struct Query
{
  char * sqlQuery ;

  // Number of table in the FROM
  int nbTable ;
  // Name of the tables
  char ** tableNames ;
  // Alias of the tables
  char ** aliasNames ;

  // Number of fields that should be returned
  int nbOutputFields ;
  // Name of the fields that should be returned alias_name.column_name
  char ** outputFields ;

  // Number of restriction type alias_name.column_name = value
  int nbRestrictionsEqual ;
  // Name of the fields with this restriction
  char **  restrictionEqualFields ;
  // Value to be equal to
  Value * restrictionEqualValues ;

  // Number of restriction type alias_name.column_name > value
  int nbRestrictionsGreaterThan ;
  // Name of the fields with this restriction
  char **  restrictionGreaterThanFields ;
  // Value to be superior to 
  Value * restrictionGreaterThanValues ;

  // Number of joins
  int nbJoins ;
  // Name of the first field alias_name.column_name
  char ** joinFields1 ;
  // Name of the second field alias_name.column_name
  char ** joinFields2 ;
};

/**
 * This structure contains a table
 *
 * There is always an index on the primary key. It is not listed in
 * the list of possible other index.
 *
 * The fields beginning by an "_" are indexed. The primary key
 * will always be the first field, and will always be indexed and therefore
 * will always start with an "_".
 */
struct Table
{
  // Table name
  char * tableName ;

  // Number of field in the table
  int nbFields ;
  // Names of the fields: column_name
  char ** fieldsName ;
  // Type of the field, either INT or STRING
  ValueType * fieldsType ;

  // Number of partitions
  int nbPartitions ;
  // Partitions of the data
  Partition * partitions ;
};

/**
 * This structure contains a partition of a table
 *
 * A partition contains all the records with primary keys 
 * that are inside an interval [firstKey, lastKey]
 *
 * The union of all the partitions contain all the data.
 * The intersection of two partitions does not have to be empty:
 * A record can be in multiple partitions, the data may be replicated.
 * 
 * Multiple partitions of the same table cannot be on the same node
 *
 * A partition will always contain at least two elements.
 *
 * The index contains all the keys that are in the partition, but 
 * not the other potential keys on other partitions.
 *
 * Access to the indexes is done using the openIndex method.
 */
struct Partition
{
  // The node that has the record
  int iNode ;
  // The name of the file on the file system of the node
  char * fileName ;
};


/**
 * A node represent a unit/box/server/computer
 * 
 * For testing purpose, two different nodes can have the same ip (127.0.0.1)
 * but never the same iNode.
 *
 * The number of node will always be less than 100.
 * You are allowed to use the ports only from 10000 to 19999 included.
 *
 * The iNode goes from 0 to (NbNodes-1). A node does not necessarily holds data.
 * The node with the iNode 0 is the master and will be the only one which
 * receives the query.
 */
struct Node
{
  // IP ( ipv4 )
  char ip[15] ;
  // Arbitrary id between 0 and 99
  int iNode ;
};

/**
 * Structure representing all the nodes.
 */
struct Nodes
{
  Node * nodes ;
  int nbNodes ;
};

/**
 * Structure representing all the data.
 */
struct Data 
{
  Table * tables ;
  int nbTables ;
};

/**
 * Structure representing a set of queries.
 */
struct Queries 
{
  Query * queries ;
  int nbQueries ;
};

/**
 *
 * =========================================================================
 * The methods you have to implement 
 * =========================================================================
 *
 **/


/**
 * You will be given nbSeconds seconds to run some pre-treatment
 * before the benchmark actually starts. At this point, all the indexes
 * are already in memory.
 * This time will always be granted whether you use it or not.
 * After the nbSeconds, we will start some calls to perform queries.
 * preset is a sample set of queries; all queries run will be of this form,
 * though constant values may change.
 *
 * The number of seconds granted will always be between 1 and 60.
 *
 * The benchmark does wait for the call to startPretreatmentMaster
 * to return before starting the query load. However, if this call takes
 * more than the granted number of seconds, the excess time is measured as
 * part of the benchmark time.
 *
 * The exact order of the pretreatment calls ( master, slaves ) is
 * unpredictable.
 *
 * This method will be executed on the master node.
 */
void startPreTreatmentMaster( int nbSeconds, const Nodes * nodes, const Data * data, const Queries * preset );

/**
 * This method will be executed on all the other nodes.
 * This is the only call to a method on the slaves and should typically
 * not return before the very end of the query load
 */
void startSlave( const Node * masterNode, const Node * currentNode);

/**
 * This structure holds information about the current connection.
 * You have to define it.
 */
struct Connection ;

/**
 * Several connections will be created on the master node by the benchmark
 * process. The benchmark will create several threads. A single thread can create multiple 
 * connections. A single connection may be shared by multiple threads.
 * However, each connection can perform one single query at a time.
 * A connection will never be closed before a DB_END is returned by fetchRow.
 * A performQuery will never be executed before a DB_END is returned by fetchRow
 * ( except for the first execution or the connection ).
 *
 * The number of simultaneous connections will always be less than 50.
 *
 * These connections are independent from the eventual connections 
 * between master and slaves that you will have to create yourself.
 */
Connection * createConnection();
void closeConnection( Connection * connection) ;

/**
 * This call triggers the query.
 * Once this call returns, we will start to fetch the rows.
 * There is no timeout for this call, the only thing that will be
 * measured is the total time for all the queries to be executed.
 *
 * This method will be executed on the master node.
 *
 * Only one query will be performed on a connection at a time.
 */
void performQuery( Connection * connection, const Query * q );

/**
 * You are not responsible for the memory allocation of the values.
 * It will be preallocated. All the results have to be written in values.
 *
 * Return DB_END if there is no more tuples to be written in values, 
 * SUCCESS if the values were properly copied.
 *
 * You can return the results in any order, but you have to return
 * the right number of rows and the right values. The duplicates
 * must be handled like a typical DBMS would ( MySQL, Oracle, PostGreSQL ).
 *
 * If you return DB_END, the content in values will be ignored.
 */
ErrCode fetchRow( Connection * connection, Value * values );

/**
 * This method will be called on the master after all the queries
 * have been issued. By the end of this call you should have close
 * all your open sockets and all your threads should be killed.
 */
void closeProcess() ;

/**
 *
 * =========================================================================
 * The methods already implemented 
 * =========================================================================
 *
 **/

/**
 * The following methods are used to access the index on the nodes.
 * They cannot be redefined.
 * The index name is table_name.column_name where column_name starts
 * with a '_'. Advanced description is available in /lib/index/include/server.h
 *
 * openIndex(const char *name, Index **idxState)
 * beginTransaction(TxnState **txn)
 * commitTransaction(TxnState *txn)
 * get(Index *ident, TxnState *txn, Record *record)
 * getNext(Index *ident, TxnState *txn, Record *record)
 **/

#ifdef __cplusplus
}
#endif

#endif
