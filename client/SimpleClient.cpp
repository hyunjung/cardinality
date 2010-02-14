
#include "../include/client.h"
#include "../lib/clientCom.h"
#include "../lib/Tools.h"
#include <map>
#include <vector>
#include <cstring>
#include "SimpleClient.h"

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void startPreTreatmentMaster( int nbSeconds, const Nodes * nodes, const Data * data, const Queries * preset )
{
  sNodes = nodes ;
  sData = data ;

  for( int t = 0 ; t < data->nbTables ; t ++ )
    sTables[ string( data->tables[t].tableName ) ] = & data->tables[t]  ;

  //Open the sockets
  openPorts(nodes, &ports, 10000) ;
}

void startSlave( const Node * masterNode, const Node * currentNode )
{
  sleep(1);
  connectPorts( masterNode, currentNode, &ports, 10000 );
  char command[200] ;
  while(true)
  {
    receiveString(&ports,0, command);
    if( strcmp( command, "KILL") == 0 )
      break ;
    else
      {
        string c(command) ;
        FileReader fr( c ) ;
        while(true)
        {
          string line = fr.getStripLine() ;
          if( line.size() > 0 )
            sendString( &ports, 0, line.c_str() );
          else
          {
            sendString( &ports, 0, "EOF" ) ;
            break ;
          }
        }
      }
  }

  closePort( masterNode, &ports) ;
}

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
  pthread_mutex_lock(&mutex);
  connection->q = q ;
  connection->iResult = 0 ;
  connection->nbResult = 0 ;

  // Read the data, and write the results in the connection object
  readFromTables(connection,q,0);
  pthread_mutex_unlock(&mutex);

}

ErrCode fetchRow( Connection * connection, Value * values )
{
  if( connection->iResult == connection->nbResult ) 
    return DB_END ;
  else
  {
    for( int o = 0 ; o < connection->q->nbOutputFields ; o ++ )
    {
      values[o] = connection->results[ connection->iResult ][o] ;
    } 
    connection->iResult ++ ;
    return SUCCESS ;
  }
}

void closeProcess()
{
  for( int n = 1 ; n < sNodes->nbNodes ; n ++ )
    sendString(&ports,n,"KILL");

  sleep(1);

  closePorts(sNodes, &ports);
}



int64_t compareValue( const Value & v1, const Value & v2 )
{
  assert( v1.type == v2.type );
  if( v1.type == INT )
    return ((int64_t) v1.intVal) -((int64_t) v2.intVal ) ;
  else
    {
    return ( strcmp( v1.charVal, v2.charVal ) ) ;
    }
}

void setFields( const string & dataLine, map<string, Value> & temp, const Table * table, const char * aliasName )
{
  vector<string> parts ;
  split(dataLine,"|",parts) ;
  for( int p = 0 ; p < parts.size() ; p ++ )
  {
    Value v ; v.type = table->fieldsType[p] ;
    setValueFromString( parts[p], v) ;
    temp[ string(aliasName) +"."+ string(table->fieldsName[p]) ] = v ;
  }
}

void readFromTables( Connection * c, const Query * q, int iTable )
{
  if( iTable == q->nbTable )
  {
    bool condition = true ;

    for( int j = 0 ; j < q->nbJoins ; j ++ )
      if( compareValue( c->temp[ string(q->joinFields1[j]) ], c->temp[ string(q->joinFields2[j]) ] ) != 0 )
      {
        condition = false ;
      }

    for( int ci = 0 ; ci < q->nbRestrictionsGreaterThan ; ci ++ )
    {
      if( compareValue( c->temp[ string(q->restrictionGreaterThanFields[ci]) ], q->restrictionGreaterThanValues[ci] ) <= 0 )
        condition = false ;
    }
    
    for( int ci = 0 ; ci < q->nbRestrictionsEqual ; ci ++ )
      if( compareValue( c->temp[ string(q->restrictionEqualFields[ci]) ], q->restrictionEqualValues[ci] ) != 0 )
        condition = false ;

    if( condition )
    {
      std::vector<Value> v(10); // Max 10 column per tuple
      c->results.push_back(v);
      for( int o = 0 ; o < q->nbOutputFields ; o ++ )
      {
        c->results[c->nbResult][o] = c->temp[ string( q->outputFields[o] ) ] ;
      } 
      c->nbResult ++ ;
    }
  }
  else
  {
    string tableName( q->tableNames[iTable] );
    Table * table = sTables[ tableName ] ;
    for( int p = 0 ; p < table->nbPartitions ; p ++ )
    {
      Partition * part = &table->partitions[p] ;      
      if( part->iNode == 0 )
      {
        FileReader fr(part->fileName) ;
        while(true)
        {
          string dataLine = fr.getStripLine() ;
          if( dataLine.size() == 0 )
            break ;
          setFields( dataLine, c->temp, table, q->aliasNames[iTable] );
          readFromTables( c,q, iTable +1 );
        }
      }
      else
      {
        sendString(&ports,part->iNode,part->fileName);
        char buffer[200] ;
        while(true)
        {
          receiveString(&ports,part->iNode,buffer) ;
          if( strcmp( buffer, "EOF" ) == 0 )
            break ;
          string dataLine(buffer) ;
          setFields( dataLine, c->temp, table, q->aliasNames[iTable] );
          readFromTables( c,q, iTable +1 );
        }  
      }
    }
  } 
}


