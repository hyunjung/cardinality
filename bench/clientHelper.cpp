
#ifndef __CLIENT_HELPER__SIGMOD__10__
#define __CLIENT_HELPER__SIGMOD__10__

#include "../include/client.h"
#include "../lib/Tools.h"
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <string>

using namespace std ;

Node createNode(const char * ip, int iNode)
{
  Node out ;
  strcpy(out.ip, ip) ;
  out.iNode = iNode ;

  return out ;
}

Nodes createNodes(vector<Node> & cppNode )
{
  Nodes out ;
  out.nodes = new Node[ cppNode.size() ] ;
  for( int n = 0 ; n < cppNode.size() ; n ++ )
    out.nodes[n] = cppNode[n] ;
  out.nbNodes = cppNode.size() ;

  return out ;
}

void deleteNodes(Nodes & nodes)
{
  delete[] nodes.nodes ;
}

Partition createPartition(int iNode, int nbFields, string filename )
{
  Partition out ;
  out.iNode = iNode;
  out.fileName = new char[200] ;
  strcpy( out.fileName, filename.c_str() ) ;

  return out ;
}

Table createTable(string tableName, vector<string> & fieldsName, vector<ValueType> & fieldsType, vector<Partition> & partitions )
{
  Table out ;
  out.tableName = new char[200] ;
  strcpy( out.tableName, tableName.c_str() ) ;
  out.nbFields = fieldsName.size() ;
  out.fieldsName = new char*[fieldsName.size()] ;
  out.fieldsType = new ValueType[fieldsName.size()] ;
  for( int f = 0 ; f < fieldsName.size() ; f ++ )
  {
    out.fieldsName[f] = new char[200] ;
    strcpy( out.fieldsName[f], fieldsName[f].c_str() ) ;
    out.fieldsType[f] = fieldsType[f] ;
  }

  out.nbPartitions = partitions.size() ;
  out.partitions = new Partition[partitions.size()] ;
  for( int p = 0 ; p < partitions.size() ; p ++ )
    out.partitions[p] = partitions[p] ;

  return out ;
}

Data createData(vector<Table> & tables)
{
  Data out ;
  out.nbTables = tables.size();
  out.tables = new Table[tables.size()] ;
  for( int t = 0 ; t < tables.size() ; t ++ )
    out.tables[t] = tables[t] ;
  return out ;
}

void deletePartition(Partition & partition)
{
  delete[] partition.fileName ;
}

void deleteTable(Table & table)
{
  for( int f = 0 ; f < table.nbFields ; f ++ )
    delete[] table.fieldsName[f] ; 

  for( int p = 0 ; p < table.nbPartitions ; p ++ )
    deletePartition( table.partitions[p] );

  delete[] table.fieldsName ;
  delete[] table.fieldsType ;
  delete[] table.tableName ;
  delete[] table.partitions ;
}

void deleteData(Data & data)
{
  for( int t = 0 ; t < data.nbTables ; t ++ )
    deleteTable( data.tables[t] ) ;
  delete[] data.tables ;
}

ValueType getTypeFromString(string s)
{
  if( s.compare("int") == 0 )
    return INT ;
  else
    return STRING ;
}

Value getValueFromString(string s)
{
  s = trim(s);
  Value out ;

  if( s[0] == '\'' )
  {
    out.type = STRING ;
    string raw = s.substr(1,s.size()-2) ;
    string clean = replace( replace(raw,"\\'","'") , "\\\\","\\" ) ;
    strcpy( out.charVal, clean.c_str() ) ;
  }
  else
  {
    out.type = INT ;
    out.intVal = atoi(s.c_str()) ;
  }

  return out ;
}

Data createDataFromDirectory( string directory, vector<Node> & nodes, int & nbSeconds )
{
  vector<Table> tables ;

  FileReader reader( directory+"/structure" ) ;

  while(true)
  {
    string command = reader.getStripLine() ;
    if( command.size() == 0 )
      break ;
    if( command.compare("NODE") == 0 )
    {
      string ip = reader.getStripLine() ;
      nodes.push_back( createNode( ip.c_str(), nodes.size() ) ) ;
    }
    else if( command.compare("TABLE") == 0 )
    {
      string tableName = reader.getString() ;
      int nbFields = reader.getInt();
      vector<string> fieldsName ;
      vector<ValueType> fieldsType ;

      for( int f = 0 ; f < nbFields ; f ++ )
      {
        fieldsName.push_back( reader.getString() ) ;
        fieldsType.push_back( getTypeFromString( reader.getString() ) );
      }
      string partitionSep = reader.getString() ;
      assert( partitionSep.compare("PARTITIONS") == 0 ) ;

      vector<Partition> partitions ;
      int nbPartition = reader.getInt() ;
      for( int p = 0; p < nbPartition ; p ++ )
      {
        int iNode = reader.getInt();
        string filePart = directory+"/data/"+tableName+"/part"+getStringFromInt(p) ;
        partitions.push_back( createPartition(nodes[iNode].iNode, nbFields, filePart) ) ;
      }

      tables.push_back( createTable( tableName, fieldsName, fieldsType, partitions ) ) ;
    }
    else if( command.compare("DELAY") == 0 )
    {
      nbSeconds = reader.getInt() ;
    }
  }

  return createData( tables );
}

void createCopyTrim(const string & str, char* & out )
{
  char* newString = new char[200] ;
  out = newString ;
  strcpy( out, trim(str).c_str() );
}

void createQueryFromString(Query & query, string & querySql)
{
  query.sqlQuery = new char[200] ;
  strcpy(query.sqlQuery, querySql.c_str() );
  query.nbRestrictionsEqual = 0 ;
  query.nbRestrictionsGreaterThan = 0 ;
  query.nbJoins = 0 ;

  vector<string> kwSFW ; 
  kwSFW.push_back("SELECT") ; kwSFW.push_back("FROM") ; kwSFW.push_back("WHERE") ;

  vector<string> parts ;
  splitMultiple( querySql, kwSFW, parts) ;
  assert( parts.size() >= 4 ) ;

  vector<string> outputs ;
  split( parts[1], ",", outputs ) ;
  vector<string> tables ;
  split( parts[2], ",", tables ) ;
  vector<string> conditions ;
  split( parts[3], "AND", conditions ) ;

  query.nbOutputFields = outputs.size();
  query.outputFields = new char*[outputs.size()] ;
  for( int o = 0 ; o < outputs.size() ; o ++ )
    createCopyTrim( outputs[o], query.outputFields[o] );

  query.nbTable = tables.size() ;   
  query.tableNames = new char*[tables.size()] ;
  query.aliasNames = new char*[tables.size()] ;
  for( int t = 0 ; t < tables.size() ; t ++)
  {
    vector<string> nameAlias ;
    split( tables[t], "AS", nameAlias );
    assert( nameAlias.size() == 2 );
    createCopyTrim( nameAlias[0], query.tableNames[t] );
    createCopyTrim( nameAlias[1], query.aliasNames[t] );
  }

  for( int c = 0 ; c < conditions.size() ; c ++ )
  {
    vector<string> condition ;
    splitTerms( conditions[c], condition) ;
    if( condition[1].compare("=") == 0 )
    {
      if( condition[2][0] == '\'' or ( condition[2][0] >= '0' and condition[2][0] <= '9' ) )
        query.nbRestrictionsEqual ++ ;
      else
        query.nbJoins ++ ;
    }
    else if( condition[1].compare(">") == 0 )
    {
      query.nbRestrictionsGreaterThan ++ ;
    }
  }

  query.restrictionEqualFields = new char *[query.nbRestrictionsEqual] ;
  query.restrictionEqualValues = new  Value[query.nbRestrictionsEqual] ;
  query.restrictionGreaterThanFields = new char *[query.nbRestrictionsGreaterThan] ;
  query.restrictionGreaterThanValues = new  Value[query.nbRestrictionsGreaterThan] ;
  query.joinFields1 = new char *[query.nbJoins] ;
  query.joinFields2 = new char *[query.nbJoins] ;

  int iRestrictionsEqual = 0 ;
  int iRestrictionsGreaterThan = 0 ;
  int iJoins = 0 ;
  for( int c = 0 ; c < conditions.size() ; c ++ )
  {
    vector<string> condition ;
    splitTerms( conditions[c], condition) ;
    if( condition[1].compare("=") == 0 )
    {
      if( condition[2][0] == '\'' or ( condition[2][0] >= '0' and condition[2][0] <= '9' ) )
      {   
        createCopyTrim(condition[0] , query.restrictionEqualFields[iRestrictionsEqual]) ;
        query.restrictionEqualValues[ iRestrictionsEqual ] = getValueFromString( condition[2] ) ;
        iRestrictionsEqual ++ ;
      }
      else
      {
        createCopyTrim(condition[0] , query.joinFields1[iJoins]) ;
        createCopyTrim(condition[2] , query.joinFields2[iJoins]) ;
        iJoins ++ ;
      }
    }
    else if( condition[1].compare(">") == 0 )
    {
      createCopyTrim(condition[0] , query.restrictionGreaterThanFields[iRestrictionsGreaterThan]) ;
      query.restrictionGreaterThanValues[ iRestrictionsGreaterThan ] = getValueFromString( condition[2] ) ;
      iRestrictionsGreaterThan ++ ; 
    }
  }

}

void createQueryFromDirectory(string directory, vector<Query> & preset, vector<Query> & queries, uint64_t & nbRowsExpected, uint64_t & hashExpected )
{
  FileReader reader( directory+"/query" ) ;
  
  reader.getStripLine() ; nbRowsExpected = reader.getLong() ;
  reader.getStripLine() ; hashExpected = reader.getLong() ;
  reader.getStripLine() ; int sizePreSet = reader.getLong() ;

  for( int s = 0 ; s < sizePreSet ; s ++ )
  {
    string querySql = reader.getStripLine() ;
    Query query ;
    createQueryFromString(query,querySql); 
    preset.push_back(query) ;
  } 

  reader.getStripLine() ;
  while(true)
  { 
    string querySql = reader.getStripLine() ;
    if(querySql.size() == 0)
      break ;

    Query query ;
    createQueryFromString(query,querySql); 
    queries.push_back(query) ;
  }

}

void deleteQuery(Query & query)
{
  for( int c = 0 ; c < query.nbRestrictionsEqual ; c ++ )
    delete[] (query.restrictionEqualFields[c]) ;
  for( int c = 0 ; c < query.nbRestrictionsGreaterThan ; c ++ )
    delete[] query.restrictionGreaterThanFields[c] ;
  for( int c = 0 ; c < query.nbJoins ; c ++ )
  {
    delete[] query.joinFields1[c] ;
    delete[] query.joinFields2[c] ;
  }
  delete[] query.restrictionEqualFields ;
  delete[] query.restrictionEqualValues ;
  delete[] query.restrictionGreaterThanFields ;
  delete[] query.restrictionGreaterThanValues ;
  delete[] query.joinFields1 ;
  delete[] query.joinFields2 ;
  delete[] query.tableNames; 
  delete[] query.aliasNames;
  delete[] query.sqlQuery ;
}

Queries createQueries(vector<Query> & queries)
{
  Queries qs ; qs.nbQueries = queries.size() ;
  qs.queries = new Query[queries.size()] ;
  for( int q = 0 ; q < queries.size() ; q ++ )
    qs.queries[q] = ( queries[q] ) ;

  return qs ; 
}

#endif

