
#include "../include/client.h"

using namespace std ;

/**
 * The trivial client does nothing but it will compile, and it is semanticly
 * correct.
 *
 * It declares that every Sql query returns 0 records.
 */

void startPreTreatmentMaster( int nbSeconds, const Nodes * nodes, const Data * data, const Queries * preset )
{
}

void startSlave( const Node * masterNode, const Node * currentNode )
{
}

struct Connection
{
};

Connection * createConnection()
{
  return new Connection() ;
}

void closeConnection( Connection * connection) 
{
  delete connection ;
}

void performQuery( Connection * connection, const Query * q )
{

}

ErrCode fetchRow( Connection * connection, Value * values )
{
  return DB_END ;
}

void closeProcess()
{
}

