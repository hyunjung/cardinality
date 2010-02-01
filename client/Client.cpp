#include <cstring>
#include "../include/client.h"
#include "../lib/clientCom.h"
#include "Client.h"
#include "SeqScan.h"
#include "NLJoin.h"


void startPreTreatmentMaster( int nbSeconds, const Nodes * nodes, const Data * data, const Queries * preset )
{
  sNodes = nodes ;
  sData = data ;

  for( int t = 0 ; t < data->nbTables ; t ++ )
    sTables[ std::string( data->tables[t].tableName ) ] = & data->tables[t]  ;

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
  }

  closePort( masterNode, &ports) ;
}

Connection *createConnection()
{
    return new Connection();
}

void closeConnection(Connection *conn)
{
    delete conn;
}

void performQuery(Connection *conn, const Query *q)
{
    conn->q = q;

    boost::shared_ptr<op::Scan> right;

    // construct an execution tree
    for (int i = 0; i < q->nbTable; ++i) {
        std::string tableName(q->tableNames[i]);
        Table *table = sTables[tableName];
        Partition *part = &table->partitions[0];
        right = boost::shared_ptr<op::Scan>(new op::SeqScan(q, q->aliasNames[i], table, part->fileName));
        if (i == 0) {
            conn->root = right;
        } else {
            conn->root = boost::shared_ptr<op::Operator>(new op::NLJoin(q, conn->root, right));
        }
    }

    conn->root->print(std::cout);
    conn->root->open();
}

ErrCode fetchRow(Connection *conn, Value *values)
{
    if (conn->root->getNext(conn->tuple)) {
        conn->root->close();
        return DB_END;
    }

    for (int i = 0; i < conn->q->nbOutputFields; ++i) {
        op::ColID cid = conn->root->getOutputColID(conn->q->outputFields[i]);
        values[i].type = conn->root->getColType(conn->q->outputFields[i]);
        if (values[i].type == INT) {
            values[i].intVal = static_cast<uint32_t>(atoi(conn->tuple[cid]));
//          std::cout << values[i].intVal << "|";
        } else {
            strcpy(values[i].charVal, conn->tuple[cid]);
//          std::cout << values[i].charVal << "|";
        }
    }
//  std::cout << endl;

    return SUCCESS;
}

void closeProcess()
{
    for (int n = 1; n < sNodes->nbNodes; ++n) {
        sendString(&ports, n, "KILL");
    }
    sleep(1);

    closePorts(sNodes, &ports);
}
