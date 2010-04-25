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




#ifndef TOOLS_H_
#define TOOLS_H_

#include <cstdlib>
#include <string.h>
#include <string>
#include "constant.h"

using namespace std ;



char * charpFromString(string s)
{
  char * out = (char *) malloc( s.length() + 1 ) ;
  strcpy( out, s.c_str() );
  return out ;
}

void cpString( string s, char * p)
{
  strcpy( p, s.c_str());
}

int32_t hashKeyTool( char * key)
{
  int l = strlen( key );
  int64_t part = 1 ;
  int64_t hash = 0 ;
  for( int c = 0 ; part < PARTS || ( part / PARTS ) < 10 ; c ++ )
    {
      part *= 53 ;
      int64_t v ;
      if( c < l )
	{
	  if( key[c] < 'a' )
	    v = key[c] - 'A' ;
	  else
	    v = key[c] - 'a' +26 ;
	 
	  if( v < 0 )
	    v = 0 ;
	  if( v >= 52 )
	    v = 51 ;
	  v ++ ;
	}
      else
	{
	  v = 0 ;
	}
      
      hash = hash*53 + (int64_t) v ;
    }
	 
      
  int32_t keyOut = hash / ( part / ((int64_t) PARTS ) ) ;
  
  if( keyOut >= PARTS )
    keyOut = PARTS - 1 ;
  
  return keyOut ;
}

const char ptov[256] = { 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,26,26,26,26,26,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52,52} ;

int64_t hashKeyToolFull(const char * key)
{
  int64_t hash = 0 ;
  int c = 0 ;
  for( ; c < 7 ; c ++ )
    {
      char k = key[c] ;
      if( k == '\0' )
	break ;
      hash = hash*256 + (int64_t) k ;
    }

  //cout << hash << endl ;
  for( ; c < 7 ; c ++ )
    hash = hash*256 ;
  
  return hash ;
}

int32_t hashKeyTool( int32_t key)
{
  int32_t k = key ;
  if( key < 0 )
    key = 0 ;
  key = key / ( MAX_SHORT_KEY / PARTS ) ;
  if( key >= PARTS )
    key = PARTS - 1 ;
  return key ;
}

int32_t hashKeyTool( int64_t key)
{
  int64_t k = key ;
  if( key < 0 )
    key = 0 ;
  key = key / ( MAX_INT_KEY / ((int64_t) PARTS) ) ;
  if( key >= PARTS )
    key = PARTS - 1 ;
  return (int32_t) key ;
}


int hash(const char *str )
{
  int hash, i;
  int len = strlen( str );
  for (hash=0, i=0; i<len; ++i)
    {
      hash += str[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  
  if( hash < 0 )
    hash = -hash ;
  if( hash == 0 )
    hash ++ ;
  if( hash == MAX_HASH )
    hash = MAX_HASH-1 ;
  
  return hash ;
} 

#endif
