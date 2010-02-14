
#include "../include/client.h"
#include "../lib/clientCom.h"
#include "../lib/Tools.h"
#include <map>
#include <vector>
#include <cstring>

/**
 * This client is a very simple one that will pass very simple query.
 *
 * All the data is first fetched in memory and it is then processed
 * to see if it fits the sql condition.
 */

using namespace std ;

static const Nodes * sNodes ;
static Ports ports ;
static const Data * sData ;
static map<string, Table * > sTables ;

struct Connection
{
  const Query * q ;
  int iResult ;
  int nbResult ;

  // Temporary object that stored the mapping between column and values
  map<string,Value> temp ; 

  // Results, dynamically allocated 
  std::vector<std::vector<Value > > results;
};

/**
 * The master makes a copy of the pointers and then open its socket
 * to communicate with the slaves.
 */
void startPreTreatmentMaster( int nbSeconds, const Nodes * nodes, const Data * data, const Queries * preset );

/**
 * The slaves handle two types of query :
 *   - KILL : close the sockets and terminates
 *   - fileName : open the file and send the data stored in this file
 */
void startSlave( const Node * masterNode, const Node * currentNode );

struct Connection ;
/**
 * Create a new connection
 */
Connection * createConnection();

/**
 * Delete the connection
 */
void closeConnection( Connection * connection);

/**
 * Performs the query.
 *
 * The entire query is done before the 'fetch row' calls, and everything
 * is stored in memory ( this behavior will inevitably fail if the tables
 * get too big ).
 */
void performQuery( Connection * connection, const Query * q );

/**
 * Copy the results from the connection object to the values object
 */
ErrCode fetchRow( Connection * connection, Value * values );

/**
 * Kill the clients and then close the sockets
 * Should always be done in that order to avoid TCP delays.
 */
void closeProcess();


/**
 * Compare two values
 * Return 0 if the values are equal
 */
int64_t compareValue( const Value & v1, const Value & v2 );

/**
 * Given a line of a data file, the name of the table, the name of the alias
 * set all the values in the map<string, Value>
 */
void setFields( const string & dataLine, map<string, Value> & temp, const Table * table, const char * aliasName );

/**
 * Set recursively all the values for each record in c->temp
 */
void readFromTables( Connection * c, const Query * q, int iTable );


