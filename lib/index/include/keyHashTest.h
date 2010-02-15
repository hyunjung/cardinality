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


#ifndef KEYHASH_H_
#define KEYHASH_H_

/**
  * This file contains three objects for keys, one for each dataType.
  * There is a lot of duplication. It could be removed, but
  * it may impact the performance. These methods are really low level,
  * and may be called many times for each command.
  * The first class is what I managed to refactor without an impact of performance.
  **/

template <class dataType>
class KeyGeneric
{
 public:
  dataType key ;
  int64_t hashKey ;
  int64_t payload ; 
 
  KeyGeneric()
    {
    }
  
  void setMaxHash()
  {
  }
  
  void allocateKey( Allocator * al ) {}
  void deallocateKey( Allocator * al ) {}

};


class KeyInt : public KeyGeneric<int32_t>
{

public:
  KeyInt * next ;

  void setSmallKey( Record * r )
  {
    key = r->val.intVal ;
    hashKey = ((int64_t) key) << 32 ;
    next = NULL ;
  }

  void setFullKey( Key * k, int64_t payloadin )
  {
    key = k->intVal ;
    hashKey = ((int64_t) key) << 32 ;
    payload = payloadin ;
    next = NULL ;
  }

  void setFullKey( Record * r )
  {
    key = r->val.intVal ;
    hashKey = ((int64_t) key) << 32 ;
    payload = r->address ;
    next = NULL ;
  }

  void setRecordKey( Record * r ) const
  {
    r->val.intVal = key ;
  }

  int64_t compare( KeyInt * k)
  {
    int64_t r = k->key - key ;
    if( r == 0 )
      {
	r = k->payload - payload ;
      }
    
    return r ;
  }
  
  int64_t compareKey( KeyInt * k)
  {
    return k->key - key ;
  }

  int size()
  {
    return 4 + 8;
  }

};


class KeyString : public KeyGeneric<char *>
{

public:
  KeyString * next ;

  void setSmallKey( Record * r )
  {
    key = r->val.charVal ;
    hashKey = hashKeyToolFull(key);
    next = NULL ;
  }

  void setFullKey( Key * k, int64_t payloadin )
  {
    key = k->charVal ;
    hashKey = hashKeyToolFull(key);
    payload = payloadin ;
    next = NULL ;
  }

  void setFullKey( Record * r )
  {
    key = r->val.charVal ;
    hashKey = hashKeyToolFull(key) ;
    payload = r->address ;
    next = NULL ;
  }

  void setRecordKey( Record * r ) const
  {
    memccpy( r->val.charVal, key, '\0', MAX_VARCHAR_LEN+1 );
  }

  int64_t compare( KeyString * k)
  {
    int64_t r = k->hashKey - hashKey ;
    if( r == 0 )
      {
	r = strcmp( k->key, key ) ;
	if( r == 0 )
	  {	    
	    r = k->payload - payload ;
	  }
      }
    
    return r ;
  }
  
  int64_t compareKey( KeyString * k)
  {
    int64_t r = k->hashKey - hashKey ;
    if( r == 0 )
      {
	r = strcmp( k->key, key ) ;
      }
    return r ;
  }

  void allocateKey( Allocator * al ) 
  {
    key = al->insert( key ) ;
  }
  
  void deallocateKey( Allocator * al )
  {
    al->remove( key ) ;
  }

  int size()
  {
    return strlen( key ) + 8;
  }
};


#endif

