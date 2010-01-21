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




#ifndef PAYLOAD_H_
#define PAYLOAD_H_

#include <list>
#include <vector>
#include <assert.h>

using namespace std ;

/**
  * This class is used to allocate the memory for the payloads
  * It uses pools and linked list. The key idea is that there are 
  * 100 different linked lists, one for each length and the pools
  * are shared between each linked list.
  **/
class Allocator
{
 private:
  char * initMemory ;
  int memoryLeft ;
  char ** spaces ;
  int allocateSpace ;
  int sizeP ;

  list<char *> globalMemory ;
  int iMemory ;

 public:
  /**
    * Build the allocator
    * @param nbSpaces max length of the string to be allocated
    **/
  Allocator( int nbSpaces )
    {
      spaces = new char*[nbSpaces] ;
      for( int s = 0 ; s < nbSpaces ; s ++ )
	spaces[s] = NULL ;

      sizeP = sizeof( char * ) ;
      allocateSpace = 100000 ;
      iMemory = 0 ;
      memoryLeft = 0 ;
    }

  ~Allocator()
    {
      delete[] spaces ;
      for( list<char *>::iterator it = globalMemory.begin() ; it != globalMemory.end() ; it ++ )
	delete[] (*it) ;
    }

  /**
    * return the memory left in the allocator
    * does not count the memory deleted and reusable
    **/
  int getSpaceUsed()
  {
    return iMemory*allocateSpace ;
  }

  /** 
    * Debug
    */
  int getMemoryLeft()
  {
    return memoryLeft;
  }
  
  void out()
  {
    cout << "String Allocator" << endl ;
    cout << "Memory used : " << (iMemory*100000) + sizeof( char* ) * 10000 << endl ;
  }

  /**
    * Copy the string given and return a pointer to the copy location
    */
  char * insert( const char * c )
  {
    int l = strlen(c);
    char * r ;

    if( spaces[l] == NULL )
      {
	if( memoryLeft < l + 1 )
	  {
	    initMemory = new char[allocateSpace] ;
	    memoryLeft = allocateSpace  ;
	    globalMemory.push_back(initMemory) ;
	    iMemory ++ ;
	  }

	
	
	memcpy( initMemory, c, l+1) ;
	r = initMemory ;
	int decal = max(l+1,sizeP);
	initMemory += decal ;
	memoryLeft -= decal ;
      }
    else
      {
	char * temp = spaces[l] ;
	spaces[l] = *(char **) spaces[l] ;
	memcpy( temp, c,l+1);
	r = temp ;
      }
    return r ;
  }

  /**
    * Deallocate a copy-pointer given by the insert method 
    */
  void remove( char * c )
  {
    int l = strlen(c);
    *((char **) c) = spaces[l] ;
    spaces[l] = c ;
  }

};



/**
  * This allocator allocates one size structure, given as a template.
  * In the problem, it stores the elements of the hashTable.
  **/
template <class T>
class FixedAllocator
{
  typedef typename list<T*>::iterator listIt ;

 private:
  int sizePool ;
  T * unused ;

  T * space ;
  list<T *> globalMemory ;
  int iMemory ;
  int iLeft ;
 public:

 /** 
   * Build the allocator
   * @param sP size of the pool
   **/
 FixedAllocator(int sP)
  {
    unused = NULL ;
    sizePool = sP ;
    iLeft = sP ;
    iMemory = 0 ;
     
  }


 ~FixedAllocator()
 {
   for( listIt it = globalMemory.begin() ; it != globalMemory.end() ; it ++ )
     delete[] (*it) ;
 } 
 
  /**
    * Debug
    **/
  void out()
  {
    cout << "Fixed Allocator" << endl ;
    cout << "Memory used : " << (10000*sizeof(T*)) + (10000*iMemory*sizeof(T)) << endl ;
    cout << "Number of records : " << 10000 * iMemory << endl ;
  } 

  /**
    * Return a pointer to a usable structure
    **/
  T * get()
  {
    if( unused != NULL )
      {
	T * out = unused ;
	unused = * ( (T**) unused ) ;
        return out ;
      }
    else if( iLeft != sizePool )
      {
	iLeft ++ ;
	space ++ ;
	return space ;
      }
    else
      {
	iLeft = 1 ;
	space = new T[10000] ;
	globalMemory.push_back(space) ;
        iMemory ++ ;
	return space ;
      }
  }

  /**
    * Deallocate a structure given by a pointer obtained from the get method
    **/  
  void remove( T * el )
  {
    *((T**) el) = unused ;
    unused = el ;
  }

  /**
    * Return the space left in the current pool.
    **/
  int getSpaceLeft()
  {
    return sizePool-iLeft;
  }
};


#endif 
