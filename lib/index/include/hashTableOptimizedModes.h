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



#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <cstdlib>
#include "tools.h"
#include "constant.h"
#include "payload.h"
#include "keyHashTest.h"

using namespace std ;

/**
  * This structure holds the records of the index.
  * It is a typical dynamic hash table.
  * The hash function preserve the order. Collisions are handled with linked lists.
  * The size of the hash table doubled every time the number of elements is superior
  * to the size of the hash table.
  *
  * This version supports non uniform distribution, by changing dynamically 
  * the hashing function. With two modes, swap dynamically detected
  * in order to maintain original performance.
  * 
  **/

class BigInt
{
  uint64_t i1 ;
  uint64_t i2 ;
  uint64_t mask ;
 public:
  BigInt() {
    i1 = 0 ;
    i2 = 0 ; 
    mask = (((uint64_t) 1) << 32 ) -1 ;
  }
  
  void add( int64_t a )
  {
    i1 += ((uint64_t) a) & mask ;
    i2 += ((uint64_t) a >> 32) ;
  }

  int64_t div( int b )
  {
    return ( i2 << (32-b) ) + (i1 >> b) ;
  }

};

template <class Key>
class HashTable
{
  int Hsize ;
  int size ;
  Key ** hashTab ;  

  int64_t average ;
  int64_t deviation ;

 public:
  int nbEl ;
  int domain ;
  bool warpMode ;

  int64_t evalAverage()
  {
    BigInt aver ;
    for( int s = 0 ; s < size ; s ++ )
      {   
	Key * el = hashTab[s] ;
        while( el != NULL )
	  {
	    aver.add(el->hashKey) ;
	    el = el->next ;
	  }
      }

    return aver.div(Hsize) ;
  }

  int64_t dist( int64_t a ,int64_t b ) { return (a<b)?b-a:a-b; }

  int64_t evalDeviation(int64_t aver)
  {
    BigInt dev ;

    for( int s = 0 ; s < size ; s ++ ) 
      {   
	Key * el = hashTab[s] ;
	while( el != NULL )
	  {
	    dev.add( dist( el->hashKey, aver ) ) ;
	    el = el->next ;
	  }
      }
    
    return dev.div(Hsize) ;
  }




  int64_t projection(int64_t s) const
  {
    if( warpMode )
      {

	if( s <= average - 2*deviation )
	  return 0 ;
	if( s >= average + 2*deviation )
	  return size - 1 ;

	int64_t coef = ( ( 4 * deviation ) / size );
	if( coef == 0 )
	  coef = 1 ;
    
	int64_t r = ( s - ( average - 2*deviation ) ) / coef ;
	if( r >= size )
	  return size - 1 ;
	return r ;
      }
    else
      {
	return s >> (63-Hsize) ;
      }
  }

  void optimize()
  {
    float domainFull =  ( ((float) nbEl)/((float) domain) ) ;
    if( ! warpMode )
      warpMode = ( domainFull > WARP_MODE_THRESHOLD ) ;
    domain = 0 ;

    if( ! warpMode )
      {
	Key ** newHashTab = new Key*[size*2] ;
	Hsize ++ ;
	for( int s = 0 ; s < size ; s ++ )
	  {
	    Key ** elP = &hashTab[s] ;
	    
	    while( *elP != NULL && ( ( projection((*elP)->hashKey) & 1 ) == 0 ) )
	      {
		elP = &(*elP)->next ;
	      }
	    
	    newHashTab[2*s+1] = *elP ;
	    *elP = NULL ;
	    newHashTab[2*s] = hashTab[s] ;
	    
	    if( newHashTab[2*s] != NULL )
	      domain ++ ;
	    if( newHashTab[2*s+1] != NULL )
	      domain ++ ;
	  }
	
	delete[] hashTab ;
	hashTab = newHashTab ;
	
	size *= 2 ;
      }

    if( warpMode )
      {
	//int64_t minDist = min( average, MAX_INT_KEY - average ) ;
	//deviation = min( evalDeviation(average), minDist / 2 ) ;
	//average = evalAverage() ;
	//deviation = evalDeviation(average) ;
        //SIGMOD OPTIMIZE
	average = evalAverage() ;
	deviation = evalDeviation(average) ;
        average += deviation ;
        deviation *= 2 ;
        //cout << minDist/2 << endl ;
        //cout << average  << " " << deviation << endl ;
	Key ** newHashTab = new Key*[size*2] ;
	
	Hsize ++ ;
	size *= 2 ;
	
	Key * lastEl = NULL ;
	int lastProj = -1 ;
	
	for( int s = 0 ; s < size ; s ++ )
	  newHashTab[s] = NULL ;
	
	for( int s = 0 ; s < size/2 ; s ++ )
	  {
	    Key * elP = hashTab[s] ;
	    
	    while( elP != NULL )
	      {
		int currentProj = projection( elP->hashKey ) ;
		if( lastProj == currentProj )
		  {
		    lastEl->next = elP ;
		  }
		else
		  {
		    if( lastEl != NULL )
		      lastEl->next = NULL ;
		    lastProj = currentProj ;

		    if( newHashTab[ currentProj ] == NULL )
		      domain ++ ;

		    newHashTab[ currentProj ] = elP ;		
		  }
		
		lastEl = elP ;
		elP = elP->next ;
	      }
	  }
	lastEl->next = NULL ;
	
	delete[] hashTab ;
	hashTab = newHashTab ;
      }

  }

  inline int64_t getSize( int64_t hash )
  {
    return projection(hash) ;
  }


  HashTable()
  {
    nbEl = 0 ;
    Hsize = START_HASH_H ;
    size = 1 << START_HASH_H ;

    average = MAX_INT_KEY / 2 ;
    deviation = MAX_INT_KEY / 4 ;

    hashTab = new Key*[size];
    memset( hashTab, 0, sizeof( Key* ) * size ) ;

    warpMode = false ;
    domain = 0 ;
  }

  ~HashTable()
  {
    delete[] hashTab ;
  }

  /**
    * Return the first element of the hashTable
    */
  Key * getFirst()
  {
    for( int s = 0 ; s < size ; s ++ )
      if( hashTab[s] != NULL )
	return hashTab[s] ;

    return NULL ;
  }

  /**
    * Return the first element with the given key
    */
  Key * get( Key * key )
  {
    Key * el = hashTab[ getSize( key->hashKey ) ] ;
    while( el != NULL )
      {
	int64_t r = key->compareKey(el);

	if( r == 0 )
	  return el ;
	else if( r > 0 )
	  return NULL ;
	el = el->next ;
      }

    return NULL ;
  }
  
  /**
    * Return the first element with the given key, or if not found return the next one
    */
  Key * getNextAfterMiss( Key * key )
  {
    int hCurrent = getSize( key->hashKey ) ;
    Key * el = hashTab[hCurrent] ;
    while( el != NULL )
      {
	if( key->compareKey(el) >= 0)
	  return el ;
	else
	  el = el->next ;
      }
    
    for( int s = hCurrent + 1 ; s < size ; s ++ )
      if( hashTab[s] != NULL )
	return hashTab[s] ;

    return NULL ;
  }

  /**
    * Return the best place to look for the next element
    */
  void getBestNext( Key * key, Key *** lastPos, Key ** lastElement )
  {
    int hCurrent = getSize( key->hashKey ) ;
    Key ** el = &hashTab[hCurrent] ;
    while( *el != NULL )
      {
	if( key->compareKey(*el) >= 0)
	  {
	    *lastPos = el ;
	    *lastElement = *el ;
	    return ;
	  }
	else
	  el = &(*el)->next ;
      }

    if( hCurrent+1 == size )
      {
	*lastPos = NULL ;
	*lastElement = NULL ;
      }
    else
      {
	*lastPos = & hashTab[ hCurrent+1 ] ;
	*lastElement = hashTab[ hCurrent+1 ];
      }
  }

  /**
    * Return the following element of a given element
    */
  Key * getNext( Key * key )
  {
    Key * el = key->next ;
    if( el != NULL )
      return el ;
    
    for( int s = getSize( key->hashKey ) + 1 ; s < size ; s ++ )
      if( hashTab[s] != NULL )
	return hashTab[s] ;

    return NULL ;
  }

  Key * getNext( Key ** key )
  {
    if( key == NULL )
      return NULL ;

    while( * key == NULL && key != & hashTab[size] )
      key ++ ;

    if( key == & hashTab[size] )
      return NULL ;

    return *key ;
  }


  /**
    * Insert an element in the hashTable, return false if the element is already here, true otherwize
    */
  bool insert( Key * key )
  {
    Key ** elP = &hashTab[ getSize( key->hashKey ) ] ;

    if( *elP == NULL )
      domain ++ ;

    while( *elP != NULL )
      {
	int64_t r = key->compare(*elP) ;
	if( r == 0 )
	  return false ;
	else if( r > 0 )
	  break ;
	else
	  elP = &(*elP)->next ;
      }

    key->next = *elP ;
    *elP = key ;
    nbEl ++ ;
    
    if( nbEl >> OPTIMIZE_THRESHOLD  >= size )
      optimize() ;

    return true ;
  }

  /**
    * Debug
    */
  void out()
  {
    cout << "warpMode : "<< warpMode << endl ;
    cout << "domain : " << domain << endl ;
    cout << "nbEl : " << nbEl << endl ;
    cout << average << endl ;
    cout << deviation << endl ;
/*
    for( int s = 0 ; s < size ; s ++ )
      {
	Key * el = hashTab[ s ] ;
	while( el != NULL )
	  {
	    cout << s << ":" << el->key << " " << el->hashKey << endl ;
	    el = el->next ;
	  }
      }
*/    
  }

  void outSize()
  {
    int S = 0 ;

    for( int s = 0 ; s < size ; s ++ )
      {
	Key * el = hashTab[ s ] ;
	while( el != NULL )
	  {
	    S += el->size(); 
	    el = el->next ;
	  }
      }
    
    cout << "HashTable" << endl; 
    cout << "Memory inserted : "<< S << endl ;
    cout << "Number of records : " << nbEl << endl ;
    cout << "Memory used : " << sizeof(Key*) * size << endl ;
  }

  /**
    * Debug
    */
  void out( Key * key)
  {
     Key * el = hashTab[ getSize( key->hashKey ) ] ;
     cout << getSize( key->hashKey, Hsize ) << " :  " ;
     while( el != NULL )
       {
	 cout << el->key << " " ;
	 el = el->next ;
       }
     cout << endl ;
  }


  /**
    * Return the average number of jumps to get an element. This should never be greater than 5 or 6.
    **/
  float collision()
  {
    int k = 0 ;
    int nbEl = 0 ;
    for( int s = 0 ; s < size ; s ++ )
      {
	Key * el = hashTab[s] ;
	int p = 1 ;
	while( el != NULL )
	  {
	    k += p ;
	    el = el->next ;
	    p ++ ;
	    nbEl ++ ;
	  }
      }

    return ((float) k)/((float) nbEl) ;
  }

  /**
    * Removes all the elements with a given key from the hashTable. 
    * Return the first element of the list of deleted elements
    * Return NULL if not found.
    **/
  Key * removeAll( Key * key )
  {
    Key ** elP = &hashTab[ getSize( key->hashKey ) ] ;
    Key ** memo = elP ;

    while( *elP != NULL )
      {
	int64_t r = key->compareKey(*elP) ;
	if( r == 0 )
	  {
	    Key ** redirect = elP ;
	    Key * out = *elP ;
	    
	    elP = &(*elP)->next ;
	    nbEl -- ;

	    while( *elP != NULL && key->compareKey(*elP) == 0 )
	      {
	      elP = &(*elP)->next ;
	      nbEl -- ;
	      }

	    *redirect = *elP ;
	    *elP = NULL ;
	    
	    if( *memo == NULL )
	      domain -- ;

	    return out ;
	  }
	else if( r > 0 )
	  return NULL ;
	else
	  elP = &(*elP)->next ;
      }
    
    return NULL ;
  }

  /**
    * Remove a couple (key, payload) from the hash table. 
    * Return null if not found.
    **/
  Key * removeOne( Key * key )
  {
    Key ** elP = &hashTab[ getSize( key->hashKey ) ] ;
    Key ** memo = elP ;

    while( *elP != NULL )
      {
	int64_t r = key->compare(*elP) ;
	if( r == 0 )
	  {
	    Key * out = *elP ;
	    *elP = (*elP)->next ;
	    out->next = NULL ;

	    nbEl -- ;
	    if( *memo == NULL )
	      domain -- ;

	    return out ;
	  }
	else if( r > 0 )
	  return NULL ;
	else
	  elP = &(*elP)->next ;
      }
    
    return NULL ;
  }

};


#endif


 
