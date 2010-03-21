
#include <iostream>
#include <vector>
#include <map>
#include "../include/client.h"
#include "../lib/Tools.h"
#include "../lib/clientCom.h"
#include "clientHelper.h"
#include "clientIndex.h"
#include <assert.h>
#include <time.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/timeb.h>
#include <sys/resource.h>
#include <sys/stat.h>

using namespace std ;

#define DEBUG 2

struct ThreadContext
{
  int nbSeconds ;
  Ports * ports ;
  Nodes * nodes ;
  Data * data ;
  Queries * preset ;
};

static ThreadContext tc ;

static void *startTreatment(void *arg)
{
  uintptr_t iNode = static_cast<int>(reinterpret_cast<intptr_t>(arg));
  if( iNode == 0 )
  {
    startPreTreatmentMaster( tc.nbSeconds, tc.nodes, tc.data, tc.preset );
  }
  else
  {
    string command = "PRETREATMENT" ;
    sendString(tc.ports, iNode, command.c_str() );
  }
}

pthread_t startThreadedPretreatment(int nbSeconds, Ports * ports, Nodes * nodes, Data * data, Queries * preset )
{
  tc.nbSeconds = nbSeconds ;
  tc.ports = ports ;
  tc.nodes = nodes ;
  tc.data = data ;
  tc.preset = preset ;

  pthread_t *threads = new pthread_t[ nodes->nbNodes ] ;
  for( int n = 0 ; n < nodes->nbNodes ; n ++ )
    pthread_create(&threads[n], NULL, startTreatment, reinterpret_cast<void*>(static_cast<uintptr_t>(n))) ;

  return threads[0];
}




int main(int argc, char ** argv)
{
  if( argc < 3)
  {
    cout << "Usage : [data directory] [local or distant] " << endl ;
    return 0 ;
  }

  string nameMaster( argv[0] );
  string testDir( argv[1] ) ;
  string mode( argv[2] ) ;

  #if DEBUG >= 1
  cout << "Start initialization" << endl ;
  #endif

  vector<Node> cppNodes ;
  vector<Query> queries ;
  vector<Query> cppPreset ;

  int nbSeconds = 0 ;
  uint64_t nbRowsExpected ; uint64_t hashExpected ;

  // Create the C structure
  Data data = createDataFromDirectory(testDir,cppNodes, nbSeconds) ;
  createQueryFromDirectory(testDir, cppPreset, queries, nbRowsExpected, hashExpected);
  Nodes nodes = createNodes( cppNodes ) ; 
  Queries preset = createQueries( cppPreset );
  Ports ports ;

  
  // Launch the clients
  for( int n = 1 ; n < nodes.nbNodes ; n ++ )
  {
    char command[2000] ;
    if( mode.compare("local") == 0 )
      sprintf(command,"nohup ./objs/mainSlave%s %s %i %s %i > logs/%i.log 2> logs/%i.error &",nameMaster.substr(11).c_str(),  nodes.nodes[0].ip, 0, nodes.nodes[n].ip, n, n,n);
    else if( mode.compare("distant") == 0 )
    {
      char file[4000] ;
      sprintf(file,"cd %s\nnohup %s/objs/mainSlave%s %s %i %s %i > %s/logs/%i.log 2> %s/logs/%i.error &",getenv("PWD"),getenv("PWD"),nameMaster.substr(11).c_str(), nodes.nodes[0].ip, 0, nodes.nodes[n].ip, n, getenv("PWD"), n, getenv("PWD"),n);
      FILE* fd = fopen("objs/tmp","w");
      fwrite(file,strlen(file),1,fd);
      fclose(fd);
      sprintf(command,"ssh %s < objs/tmp > logs/ssh.log 2> logs/ssh.error", nodes.nodes[n].ip);
    }
    else 
    {
      cout << "Usage : [data directory] [local or distant] " << endl ;
      return 0 ;
    }

    #if DEBUG >= 2
    cout << command << endl ;
    #endif
    system(command) ;
  }

  cout << "Opening Ports" << endl ;
  // Open the master ports
  openPorts(&nodes, &ports, 20000) ;

  cout << "Creating Indexes" << endl ;
  // Create the indexes
  std::map<int,int> nbIndexes;
  for( int t = 0 ; t < data.nbTables ; t ++ )
    for( int p = 0 ; p < data.tables[t].nbPartitions ; p ++ )
      for( int f = 0 ; f < data.tables[t].nbFields ; f ++ )
      {
        int n = data.tables[t].partitions[p].iNode ;
        if( data.tables[t].fieldsName[f][0] == '_' )
        {
          char fullFieldName[500] ;
          sprintf(fullFieldName,"%s.%s", data.tables[t].tableName, data.tables[t].fieldsName[f]) ;
          if( n == 0 )
          {
            createIndex( fullFieldName, data.tables[t].partitions[p].fileName, f, data.tables[t].fieldsType[f] ) ;     
          }
          else
          {
            string command = "CREATE INDEX" ;
            sendString(&ports,n,command.c_str()) ;
            sendString(&ports,n,fullFieldName) ;
            sendString(&ports,n,data.tables[t].partitions[p].fileName) ;
            sendInt(&ports,n, data.tables[t].fieldsType[f] ) ;
            sendInt(&ports,n,f);
            ++nbIndexes[n];
          }
        }
      }

  for( int n = 1 ; n < nodes.nbNodes ; n ++ ) {
    for(int k = 0 ; k < nbIndexes[n]; ++k) {
      char ack[500];
      receiveString(&ports,n,ack);
      assert(!strcmp(ack,"ACK"));
    }
  }

  #if DEBUG >= 1
  cout << "End initialization" << endl ;
  cout << "Start pretreatment" << endl ;
  #endif

  // Start the prelaunch on the master
  pthread_t masterPretreatmentThread = 
    startThreadedPretreatment( nbSeconds, &ports, &nodes, &data , &preset );
  sleep(nbSeconds) ;
  struct timezone tz;
  struct timeval start, end ;
  gettimeofday(&start,&tz);

  // Wait for the master pretreatment thread to end before going on
  pthread_join(masterPretreatmentThread,NULL);

  Connection * connection = createConnection() ;

  #if DEBUG >= 1
  cout << "End pretreatment" << endl ;
  cout << "Start queries" << endl ;
  #endif

  int64_t hash = 0 ;
  int nbRows = 0 ;

  for( int q = 0 ; q < queries.size() ; q ++ )
  { 
    Query query = queries[q] ;

    #if DEBUG >= 1
    cout << "START QUERY "<< query.sqlQuery << endl ;
    #endif
    performQuery(connection, &query) ;

    // Collect the results
    Value * values = new Value[query.nbOutputFields] ;
    while( fetchRow( connection, values ) != DB_END ) 
    { 
      for( int v = 0 ; v < query.nbOutputFields ; v ++ )
        hash = (hash + hashValue( values[v] ) ) % 1000001 ;
      nbRows ++ ;
    }
    delete[] values ;
  }

  // Measure the time
  gettimeofday(&end,&tz);
  int nbMillis = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000 ;

  #if DEBUG >= 1
  cout << "End queries" << endl ;
  #endif

  #if DEBUG >= 0
  if( nbRows == nbRowsExpected and hash == hashExpected )
    cout << "Correctness : success" << endl ;
  else
  {
    cout << "Correctness : failure " ;
    cout << "( rows returned " << nbRows << ", rows expected " << nbRowsExpected << ", " ;
    cout << "hash returned " << hash << ", hash expected " << hashExpected << ")" << endl ;
  }
  cout << "Time response : " << nbMillis << " ms" << endl ;
  #endif

  // Close the connection
  closeConnection(connection);

  // Close the application
  closeProcess();

  // Kill the clients
  for( int n = 1 ; n < nodes.nbNodes ; n++ )
    sendString(&ports,n, "KILL") ;

  sleep(1);

  // Close the ports
  closePorts(&nodes, &ports);

  deleteNodes(nodes);
  deleteData(data) ;
  for( int q = 0 ; q < queries.size() ; q ++ )
    deleteQuery(queries[q]) ;
}

