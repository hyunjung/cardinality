
#include <iostream>
#include "clientHelper.h"
#include "clientIndex.h"
#include "../lib/clientCom.h"
#include "../include/client.h"
#include <vector>

using namespace std ;

Ports ports ;
vector<Index *> indexes ;

int main(int argc, char ** argv)
{
  // Give the time to the master to open its port
  // Will not crash if removed, just may take more time
  sleep(1);

  assert( argc == 5 );
  Node masterNode = createNode(argv[1], atoi(argv[2]) ) ;
  Node currentNode = createNode(argv[3], atoi(argv[4]) ) ;

  // Connection to the master 
  connectPorts( &masterNode, &currentNode, &ports, 20000 );

  string ack = "ACK" ;

  char buffer[4096];
  while(true)
  {
    receiveString(&ports,0,buffer);
    cout << "Receive " << buffer << endl ;

    if( strcmp("KILL",buffer) == 0 or strcmp("ERROR", buffer) == 0 )
      break ;
    else if(strcmp("CREATE INDEX", buffer) == 0 )
    {
      char indexName[4096] ;
      char fileName[4096] ;
      int nbFields ;

      receiveString(&ports,0,indexName);
      receiveString(&ports,0,fileName);
 
      ValueType type = (ValueType) receiveInt(&ports,0) ;
      int iIndex = receiveInt(&ports,0);
      createIndex( indexName, fileName, iIndex, type) ;      
      sendString(&ports,0,ack.c_str());
    }
    else if(strcmp("PRETREATMENT",buffer) == 0)
    {
      startSlave(&masterNode, &currentNode);
    }
  }

  // Close connection to the master
  closePort( &masterNode, &ports) ;
}

