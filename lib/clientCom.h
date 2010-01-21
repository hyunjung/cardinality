
#ifndef __CLIENT_COM__SIGMOD__10__
#define __CLIENT_COM__SIGMOD__10__

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cstdlib>
#include <string>
#include "assert.h"
#include "../include/client.h"

using namespace std ;

/**
 * The following are currently used in the sample clients and the benchmark
 * to communicate between the nodes. You do not have to use these methods.
 **/

/**
 * This structure contains all the file descriptor of the open sockets
 */
struct Ports
{
  int sockfd[100] ;
};

/**
 * Open sockets to all nodes but the first, opening the ports number basePort+iNode
 * This method is currently used on the master
 */
void openPorts(const Nodes * nodes, Ports * ports, int basePort);
void closePorts(const Nodes * nodes, Ports * ports);

/**
 * Connect to a distant socket
 * This method is currently used on the slave
 */
void connectPorts(const Node * masterNode, const Node * currentNode, Ports * ports, int basePort);
void closePort(const Node * node, Ports * ports);

/**
 * These methods are used to exchange data using the Ports structure
 * There are socket based, so everything is synchronous.
 */
void receiveString(Ports * ports, int nodeFrom, char * str);
void sendString(Ports * ports, int nodeTo, const char * str);
void sendInt(Ports * ports, int nodeTo, int p );
int receiveInt(Ports * ports, int nodeFrom);

#endif

