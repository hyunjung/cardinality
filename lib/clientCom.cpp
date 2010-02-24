
#include "clientCom.h"
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/tcp.h>

using namespace std ;

// Portability issues
#define bzero(b,len) (memset((b), '\0', (len)), (void) 0)
#define bcopy(b1,b2,len) (memmove((b2), (b1), (len)), (void) 0)

void openPorts( const Nodes * nodes, Ports * ports, int basePort)
{
  for( int n = 1 ; n < nodes->nbNodes ; n ++ )
  {
    int sockfd, newsockfd, portno, clilen;
    struct sockaddr_in serv_addr, cli_addr;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    assert( sockfd >= 0 ) ; 

    bzero((char *) &serv_addr, sizeof(serv_addr));
    portno = basePort + nodes->nodes[n].iNode ;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    cout << "Bind on "<< portno << endl ;

    int attempt ; 
    for( attempt = 0 ; attempt < 30 and bind(sockfd, (struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0 ; attempt ++ ) 
    {
      sleep(2) ;
    }

    assert( attempt < 30 ) ;

    listen(sockfd,1);

    clilen = sizeof(cli_addr);

    cout << "Accept on "<< portno << endl ;

    ports->sockfd[ nodes->nodes[n].iNode ] = accept(sockfd, (struct sockaddr *) &cli_addr, (socklen_t*) &clilen);
  }

}

void connectPorts(const Node * masterNode, const Node * currentNode, Ports * ports, int basePort)
{
  int sockfd, portno ;
  struct sockaddr_in serv_addr;
  struct hostent *server;

  portno = basePort + currentNode->iNode ;
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  server = gethostbyname( masterNode->ip );

  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
  serv_addr.sin_port = htons(portno);
  
  cout << "Connect on "<< portno << endl ;

  int attempt ; 
  for( attempt = 0 ; attempt < 60 and connect(sockfd, (sockaddr*)&serv_addr,sizeof(serv_addr)) < 0 ; attempt ++ ) 
  {
    sleep(2) ;
  }

  assert( attempt < 60 ) ;

  ports->sockfd[0] = sockfd ;
}

void closePorts(const Nodes * nodes, Ports * ports)
{
  for( int n = 0 ; n < nodes->nbNodes ; n ++ )
    close( ports->sockfd[ nodes->nodes[n].iNode ] ) ;
}

void closePort(const Node * node, Ports * ports)
{
  close( ports->sockfd[ node->iNode ] ) ;
}

void sendString(Ports * ports, int iNode, const char * command)
{
  #if DEBUG >= 2
  cout << "SENDING " << command << endl ;
  #endif
  int len = strlen(command)+1 ;
  send( ports->sockfd[iNode], (char*) &len, 4, 0 ) ;
  send( ports->sockfd[iNode], command, len, 0 ) ;
}

void receiveString(Ports * ports, int iNode, char * out)
{
  int nbChar = -1 ;

  nbChar = recv( ports->sockfd[iNode], out, 4, 0) ;
  nbChar = recv( ports->sockfd[iNode], out, *(int*)out, 0 );

  if( nbChar == -1 )
    sprintf(out, "ERROR") ;

  #if DEBUG >= 2
  cout << "RECEIVING " << out << endl;
  #endif
}

void sendInt(Ports * ports, int iNode, int value)
{
  char buffer[4096] ;
  sprintf(buffer,"%d",value);
  sendString(ports,iNode,buffer);
}

int receiveInt(Ports * ports, int iNode)
{
  char buffer[4096] ;
  receiveString(ports,iNode,buffer);
  return atoi(buffer) ;
}


