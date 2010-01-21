/*


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


#ifndef HASHSHARED_H_
#define HASHSHARED_H_

#include <cstdlib>
#include <string.h>
#include <string>
#include "server.h"
#include "constant.h"
#include "tools.h"
#include <sched.h>

using namespace std ;

class basicHash
{
 public:
  int * nbNameIndex ;
  
  void clear()
  {
    for( int i = 0 ; i < HASH_SIZE_NAME_INDEX ; i ++ )
      nbNameIndex[i] = 0 ;
  }

  int collision()
  {
    int sum = 0 ;
    for( int i = 0 ; i < HASH_SIZE_NAME_INDEX ; i ++ )
      sum += nbNameIndex[i] ;
    
    return sum ;
  }

};

/**
  * This hash table is a almost thread safe hash table to access the index pointer from their name
  **/
class HashShared : public basicHash
{
 private:
  struct NameIndex
  {
    Index * idx ;
    char * str ;
  };

  NameIndex * ni ;
 

 public:
  HashShared()
  {
    ni = new NameIndex[HASH_SIZE_NAME_INDEX * MAX_INDEX] ;
    nbNameIndex = new int[HASH_SIZE_NAME_INDEX] ;
    clear();
  }

  ~HashShared()
  {
    
    delete[] ni ;
    delete[] nbNameIndex ;
    
  }

  /**
    * Insert a index in the hash table. This is not a thread safe operation.
    * Only one insert should be done at a time.
    **/
  char * insert( char * str, Index * idx )
  {
    int hashStr = hash(str) % HASH_SIZE_NAME_INDEX ;
    char * p = new char[LENGTH_INDEX_NAME] ;
    memccpy( p, str, '\0', LENGTH_INDEX_NAME);

    NameIndex & newNameIndex = ni[hashStr*MAX_INDEX + nbNameIndex[hashStr] ] ;
    newNameIndex.idx = idx ;
    newNameIndex.str = p ;

    nbNameIndex[hashStr] ++ ;

    return p ;
  }

  /**
    * Return an index from its name.
    * Multiple get and one insert can occur at the same time.
    **/  
  Index * get( const char * str )
  {
    int hashI = hash(str) % HASH_SIZE_NAME_INDEX ;
    int hashStr = ( hashI ) * MAX_INDEX ;
    for( int i = 0 ; i < nbNameIndex[hashI] ; i ++ )
      if( strcmp( str, ni[hashStr + i].str ) == 0 )
	return ni[hashStr + i].idx ;

    return NULL ;
  }



};

/**
  * This thread-safe structure is used to associate a structure to a thread.
  * It supports multiple get at the same time without mutexes.
  **/
template <class T>
class HashOnlyGet : public basicHash
{
 private:
  struct IdData
  {
    pthread_t pt ;
    T * data ;
  };

  IdData * id ;
  pthread_mutex_t mutex ;
  int iThread ;
  int nbElement ;

 public:
  HashOnlyGet()
  {
    id = new IdData[HASH_SIZE_NAME_INDEX * MAX_INDEX] ;
    nbNameIndex = new int[HASH_SIZE_NAME_INDEX] ;
    pthread_mutex_init( &mutex, NULL);
    clear();
    iThread = 0 ;
    nbElement = 0  ;
  }

  ~HashOnlyGet()
  {
    
    for( int i = 0 ; i < HASH_SIZE_NAME_INDEX ; i ++ )
      for( int j = 0 ; j < nbNameIndex[i] ; j ++ )
        delete id[ i * MAX_INDEX + j ].data ;
    delete[] id ;
    delete[] nbNameIndex ;
    
  }

  void clear()
  {
    for( int i = 0 ; i < HASH_SIZE_NAME_INDEX ; i ++ )
      nbNameIndex[i] = 0 ;
  }
 
  /**
    * Return a given structure that is specific to the current thread.
    **/
  T * get()
  {
    int64_t hash = ((int64_t) pthread_self()) % HASH_SIZE_NAME_INDEX ;
    int64_t hashReal = ( hash ) * MAX_INDEX ;

    for( int i = 0 ; i < nbNameIndex[hash] ; i ++ )
      if( id[hashReal+i].pt == pthread_self() )
	return id[hashReal+i].data ;
    
    pthread_mutex_lock(&mutex);

    IdData & out = id[ hashReal + nbNameIndex[hash] ] ;
    out.pt = pthread_self();
    out.data = new T(nbElement);
    nbNameIndex[hash] ++ ;
    nbElement ++ ;

    pthread_mutex_unlock(&mutex);

    return out.data ;
  }

};

#endif
