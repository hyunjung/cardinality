/*
 *


The MIT License

Copyright (c) 2009 Clement Genzmer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


*/



#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "include/server.h"
#include "include/payload.h"
#include "include/tools.h"
#include "include/constant.h"
#include "include/keyHashTest.h"
#include "include/hashSharedNumbered.h"
#include "include/hashTableOptimizedModes.h"
#include "include/deadLockDetectorDynamic.h"

#define TRACE 0

using namespace std ;

DeadLockDetector * detector ;

class Index
{
public:
  virtual ErrCode get(TxnState *txn, Record *record) = 0 ;
  virtual ErrCode getNext(TxnState *txn, Record *record) = 0 ;
  virtual ErrCode fastInsertRecord(Key *k, int64_t payload) = 0 ;
  virtual ErrCode insertRecord(TxnState *txn, Key *k, int64_t payload) = 0 ;
  virtual ErrCode deleteRecord(TxnState *txn, Record *theRecord) = 0 ;

  virtual void abort(TxnState *txn) = 0 ;
  virtual void commit(TxnState *txn) = 0 ;
  virtual void out() = 0 ;
  virtual void setName(string n, int in) = 0 ;  

};


/**
  * The transaction state records every index the transaction uses.
  **/
class TxnState
{
public:
  int64_t state ;
  int64_t indexActive[MAX_INDEX] ;
  int64_t indexToReset[MAX_INDEX] ;
  int64_t nbIndex ;
  int64_t th ;
  int64_t iNbR ;

  TxnState(int thin){
    th = thin ;

    for( int i = 0 ; i < MAX_INDEX ; i ++ )
      indexActive[i] = 0 ;
    state = 1 ;
    nbIndex = 0 ;
    iNbR = 0 ;
  }

  /**
    * Reset the state, ready for a new transaction
    **/
  void reset()
  {
    nbIndex = 0 ;
    iNbR = 0 ;
    state ++ ; 
  }

  /**
    * Add a new index given by its number
    * Return true if the index was acquired, false if it was already acquired
    **/
  bool addIndex( int i )
  {
    if( indexActive[i] != state )
      {
	indexActive[i] = state ;
	indexToReset[nbIndex] = i ;
	nbIndex ++ ;
	return true ;
      }
    return false ;
  }

  bool locked( int i )
  {
    return ( indexActive[i] == state ) ;
  }
};

/**
  * This structure is the high level index.
  **/
template <class KeyType>
class indexTyped : public Index
{

  struct action
  {
    bool isInsert ;
    KeyType * key ;
  };
  
private:
  HashTable<KeyType> * ht ;
  FixedAllocator<KeyType> * keyAllocator ;
  Allocator * al ;

  string name ;

  KeyType ** lastPos ;
  KeyType * lastElement ;
  bool getDone ;
  bool getFound ;

  action actions[MAX_ACTION_PER_TRANSACTION] ;
  int nbActions ;

  bool locked ;
  pthread_mutex_t mutex ;

  char * probe ;

public:
  int iname ;

  /** 
    * Lock the index, if thread has not already locked it
    **/
  bool lock(TxnState * txn)
  {
    if( ! txn->locked(iname) )
      {
	if( txn->nbIndex == 0 && detector->filtering == false )
	  {
	    txn->addIndex(iname);
	    detector->acquireIndex( txn->th, iname );
	    return true ;
	  }
	else
	  {
	    bool result = detector->askingIndex( txn->th, iname, txn->nbIndex + 1 ) ;
	    if( result )
	      {
		txn->addIndex(iname) ;
		detector->acquireIndex( txn->th, iname );
	      }
	    return result ;
	  }
      }
    
    return true ;
  }

  /**
    * Unlock the index
    **/
  void unlock(TxnState * txn)
  {
    resetNext() ;
    txn->iNbR ++ ;
    detector->releaseIndex( txn->th, iname, txn->nbIndex - txn->iNbR ) ;
  }

  indexTyped()
  {
    pthread_mutex_init(&mutex,NULL);

    ht = new HashTable<KeyType>();
    keyAllocator = new FixedAllocator<KeyType>(SIZE_KEY_POOL); 
    al = new Allocator( MAX_LENGTH_ALLOC ) ;

    nbActions = 0 ;
    getFound=false;
    getDone=false;
    lastElement=0;
    lastPos=0;
    locked=0;

    //probe = new char[1<<20];
    //memset( probe, 0 , 1 << 20 );

  }

  ~indexTyped()
  {
    delete ht ;
    delete keyAllocator ;
    delete al ;
  }

  /**
    * Set the name of the index, and id
    **/
 
  void setName(string n, int in){ name = n ; iname = in ; }

  /**
    * Reset the state of the index, to be ready for the next transaction
    * Should be called at the end of each transaction
    **/
  void resetNext()
  {
    getDone = false ;
    nbActions = 0 ;
  }

  /**
    * Process a get command. Remembers the next element if necessary.
    **/
  ErrCode get(TxnState *txn, Record *record)
  { 
    if( ! lock(txn) )
      return DEADLOCK ;

    KeyType k ;
    k.setSmallKey(record);
 
    if( ht->nbEl == 0 )
      return KEY_NOTFOUND ;

    ht->getBestNext(&k,&lastPos, &lastElement);
    getDone = true ;
    
    if(  lastElement != NULL && lastElement->compareKey( &k ) == 0 )
      {
	getFound = true ;
	record->address = lastElement->payload ;
	return SUCCESS ;
      }
    else
      {
	getFound = false ;
	return KEY_NOTFOUND ;
      }
    
  }

  /**
    * Process a get next command on the current index.
    **/
  ErrCode getNext(TxnState *txn, Record *record)
  {
    if( ! lock(txn) )
      return DEADLOCK ;

    if( ! getDone )
      {
	lastElement = ht->getFirst() ;
      }
    else if( getDone && getFound )
      {
	lastElement = ht->getNext( lastElement ) ;
      }
    else
      {
	lastElement = ht->getNext( lastPos );
      }

    if( lastElement == NULL ){
      getFound = false ;
      return DB_END ;
    }
    
    getDone = true ;
    getFound = true ;

    lastElement->setRecordKey( record );
    record->address = lastElement->payload ;

    return SUCCESS ;
  }

  /** 
    * Add an action to the journal, used in case of abort.
    **/
  inline void addAction( const bool & isInsert, KeyType* & k )
  {
    actions[nbActions].isInsert = isInsert ;
    actions[nbActions].key = k ;
    nbActions ++ ;
  }

  /**
    * Insert a record in the current index
    **/
  ErrCode insertRecord( TxnState *txn, Key *kin, int64_t payload)
  {
    if( ! lock(txn) )
      return DEADLOCK ;

    KeyType * k = keyAllocator->get()  ;
    k->setFullKey( kin, payload );
    k->allocateKey(al) ;

    if( ! ht->insert(k) )
      {
	k->deallocateKey(al);
	keyAllocator->remove(k);
	return ENTRY_EXISTS ;
      }

    addAction( true, k );

    #if VERBOSE
      cout << "endInsert" << endl ;
    #endif

    return SUCCESS;
  }
 
  /**
    * Optimization for one command transaction
    **/
  ErrCode fastInsertRecord( Key *kin, int64_t payload)
  {
    ErrCode err ;

    pthread_mutex_lock( & ( detector->indexFinalLock[iname] ) );   

    KeyType * k = keyAllocator->get()  ;
    k->setFullKey( kin, payload );
    k->allocateKey(al) ;

    if( ! ht->insert(k) )
      {
	k->deallocateKey(al);
	keyAllocator->remove(k);
	err = ENTRY_EXISTS ;
      }
    else
      err = SUCCESS ;
 
    pthread_mutex_unlock( & ( detector->indexFinalLock[iname] ) );   
    
    return err;
  }

  /**
    * Delete the record from the index
    **/ 
  ErrCode deleteRecord( TxnState *txn, Record *record)
  {
    if( ! lock(txn) )
      return DEADLOCK ;

    KeyType k ;

    k.setFullKey( record );
	
    KeyType * el = ht->removeOne( &k );
    if( el == NULL )
        return ENTRY_DNE ;
    else
	addAction( false, el );
   
    return SUCCESS ;
  }
  
  /**
    * Debug
    **/
  void out()
  {
    ht->out();
  }


  /**
    * Abort the current transaction, reverse the inserts/deletes, free the memory, and unlock the index
    **/
  void abort(TxnState * txn)
  {

    for( int ia = nbActions-1 ; ia >= 0 ; ia -- )
      {
	action & a = actions[ia] ;
	if( a.isInsert )
	  {
	    ht->removeOne( a.key  ) ;
	    a.key->deallocateKey(al);
	    keyAllocator->remove(a.key);
	  }
	else
	  {
	    KeyType * el = a.key ;
	    while( el != NULL )
	      {
		KeyType * next = el->next ;
		el->next = NULL ;
		ht->insert( el );
		el = next ;
	      }
	  }
      }
    
    unlock(txn);
  }


  /**
    * Commit the current transaction, free the memory from the deletes, 
    **/
  void commit(TxnState * txn)
  {
 
    for( int ia = 0 ; ia < nbActions ; ia ++ )
      {
	action & a = actions[ia] ;
	if( ! a.isInsert )
	  {
	    KeyType * el = a.key ;
            
	    while( el != NULL )
	      {
		KeyType * next = el->next ;
		el->next = NULL ;
		el->deallocateKey(al);
		keyAllocator->remove(el);
		el = next ;
	      }
	  }
      }

    unlock(txn);
  }


};



/**
  * This structure holds all the indexes
  **/
class indexManager
{

  Index * indexTab[MAX_INDEX] ;
  int nbIndexTab ;
  HashShared hs ;

public:
  indexManager()
  {
    nbIndexTab = 0 ;
  }

  ~indexManager()
  {
    for( int i = 0 ; i < nbIndexTab ; i ++ )
      {
   	delete indexTab[i] ;
      }
  }
  /**
    * Create an index, thread safe
    **/
  ErrCode create(KeyType type, char *name)
  {
    Index * newIndex ;
    
    if( type == INT)
      {
	newIndex = (Index *) new indexTyped<KeyInt>() ; 
      }
    else if( type == STRING)
      {
	newIndex = (Index *) new indexTyped<KeyString>() ; 
      }

    indexTab[nbIndexTab] = newIndex ;
    char * p = hs.insert( name, newIndex ) ;
    newIndex->setName(p,nbIndexTab) ;
    nbIndexTab ++ ;

    return SUCCESS ;
  }
  
  /**
    * Delete an index, for unit-tests
    **/
  ErrCode deleteDb(char *name)
  {
    Index * index = hs.get(name) ;
    if( index == NULL )
      return DB_DNE ;
    
    delete index ;

    return SUCCESS ;
  }

  /**
    * Open an index, support multiple thread access
    **/
  ErrCode openIndex(const char *name, Index **idxState)
  {
    *idxState = hs.get(name) ;
    if( *idxState == NULL )
      return DB_DNE ;
    else
      return SUCCESS ;
  }
 
  /**
    * Abort the transaction
    **/
  void abort(TxnState * txn)
  {
    for( int i = 0 ; i < txn->nbIndex ; i ++ )
      indexTab[ txn->indexToReset[i] ]->abort(txn);
    txn->reset();
  }
  
  /**
    * Commit the transaction
    **/
  void commit(TxnState * txn)
  {
    for( int i = 0 ; i < txn->nbIndex ; i ++ )
      indexTab[ txn->indexToReset[i] ]->commit(txn);
    txn->reset();
  }

  /**
    * Debug
    **/
  void out(char * n)
  {
    hs.get(n)->out() ;
  }
};


indexManager * im ;
HashOnlyGet<TxnState> * transactor ;
bool firstLaunch = true ;
bool inTransaction = false ;

pthread_mutex_t mutexTransaction = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t createIndex = PTHREAD_MUTEX_INITIALIZER;
int64_t nbTransactionDone = 0 ;

/**
  * Debug
  **/
void out( char *name)
{
  im->out(name) ;
}

/**
  * Function called just before the creation of the first index
  **/
void initFirstLaunch()
{
  im = new indexManager() ;
  transactor = new HashOnlyGet<TxnState>();
  detector = new DeadLockDetector() ;
}

ErrCode create(KeyType type, char *name)
{
  #if TRACE
    cout << "Create index ----" << name <<  endl ;
  #endif

  pthread_mutex_lock(&createIndex);

  if( firstLaunch )
    {
      initFirstLaunch();
      firstLaunch = false ;
    }
   
  ErrCode err = im->create( type, name );
  detector->nbIndex ++ ;

  pthread_mutex_unlock(&createIndex);  

  return err ;
}

ErrCode deleteDb(char *name)
{
  return im->deleteDb(name);
}

ErrCode openIndex(const char *name, Index **idxState)
{
  
  #if TRACE
    cout << "Open index ----" << name <<  endl ;
  #endif

  ErrCode err = im->openIndex( name, idxState ) ;
  return err ;
}

ErrCode closeIndex(Index *ident)
{
  return SUCCESS ;
}


ErrCode beginTransaction(TxnState **txn)
{   
  #if TRACE
    cout << "Begin ----" <<  endl ;
  #endif
 
  *txn = (TxnState * ) transactor->get() ;

  return SUCCESS;
}

ErrCode abortTransaction(TxnState *txn)
{  
  #if TRACE
    cout << "Abort ----" <<  endl ;
  #endif

  im->abort(txn);
  
  return SUCCESS;
}

ErrCode commitTransaction(TxnState *txn)
{
  #if TRACE
    cout << "Commit ----" <<  endl ;
  #endif
    
  im->commit(txn) ;

  return SUCCESS;
}
    
    
ErrCode get(Index *ident, TxnState *txn, Record *record)
{
  #if TRACE
    cout << "Get ----" <<  endl ;
  #endif

  ErrCode err ;

  if( txn == NULL )
    {
      txn = transactor->get();
      err = ident->get(txn, record );
      ident->commit(txn);
      txn->reset();
    }
  else  
    err = ident->get(txn, record );
 

  return err ;
}
    
ErrCode getNext(Index *ident, TxnState *txn, Record *record)
{
  #if TRACE
    cout << "GetNext ----" <<  endl ;
  #endif

  ErrCode err ;

  if( txn == NULL )
    {
      txn = transactor->get();
      err = ident->getNext(txn, record );
      ident->commit(txn);
      txn->reset();
    }
  else  
    err = ident->getNext(txn, record );
 

  return err ; 
}

ErrCode insertRecord(Index *ident, TxnState *txn, Key *k, uint64_t payload)
{
  #if TRACE
    cout << "Insert ----" <<  endl ;
  #endif

  ErrCode err ;

  if( txn == NULL )
    {
      err = ident->fastInsertRecord(k, payload );
    }
  else  
    err = ident->insertRecord(txn, k, payload );
  

  return err ;
}

ErrCode deleteRecord(Index *ident, TxnState *txn, Record *record)
{
  #if TRACE
    cout << "Delete ----" <<  endl ;
  #endif

  ErrCode err ;

  if( txn == NULL )
    {
      txn = transactor->get();
      err = ident->deleteRecord(txn, record );
      ident->commit(txn);
      txn->reset();
    }
  else  
    err = ident->deleteRecord(txn, record );
 

  return err ;
}

