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

 

#ifndef DEADLOCKDETECTOR_H_
#define DEADLOCKDETECTOR_H_

#include "constant.h"
#include <set>

using namespace std ;

class DeadLockDetector
{
  int index[ MAX_INDEX ] ;
  int thread[ MAX_THREADS ] ;
  pthread_mutex_t threadLock[ MAX_THREADS ] ;
  

  int nbRessourceAsked ;
  int nbThreadPermitted ;

  bool inSerializer[ MAX_THREADS ] ;
  pthread_mutex_t serializer ;
  pthread_cond_t serialCond ;

 public:
  int nbIndex ;
  bool filtering ;

  pthread_mutex_t indexFinalLock[ MAX_INDEX ] ;


  DeadLockDetector()
    {
      nbRessourceAsked = 0 ;
      nbThreadPermitted = 0 ;
      nbIndex = 0 ;
      filtering = false ;

      pthread_mutex_init( &serializer , NULL );
      pthread_cond_init( &serialCond, NULL );
 
      for( int i = 0 ; i < MAX_INDEX ; i ++ )
	{
	  index[i] = -1 ;
	  pthread_mutex_init( &indexFinalLock[i], NULL );
	}
      for( int i = 0 ; i < MAX_THREADS ; i ++ )
	{
	  thread[i] = -1 ;
	  inSerializer[i] = false ;
	  pthread_mutex_init( &threadLock[i], NULL );
	}
    }

  void acquireIndex(int th, int ind)
  {
    //cout << th << " acquire " << ind << endl ;

    pthread_mutex_lock( &indexFinalLock[ind] ) ;


    //pthread_mutex_lock( &threadLock[th] ) ;
    //pthread_mutex_lock( &indexLock[ind] ) ;
    
    index[ind] = th ;
    thread[th] = -1 ;
    
    //pthread_mutex_unlock( &threadLock[th] ) ;
    //pthread_mutex_unlock( &indexLock[ind] ) ;

    //cout << th << " acquired " << ind << endl ;

  }

  void releaseIndex(int ind)
  {
    //cout << index[ind] << " release " << ind << endl ;

    //pthread_mutex_lock( &indexLock[ind] ) ;
    index[ind] = -1 ;
    //pthread_mutex_unlock( &indexLock[ind] ) ;

    pthread_mutex_unlock( &indexFinalLock[ind] ) ;

    //cout << index[ind] << " released " << ind << endl ;

  }

  void releaseIndex(int th, int ind, int nbR)
  {
    //cout << index[ind] << " == " << th << " release " << ind << " " << nbR << endl ;

    //pthread_mutex_lock( &indexLock[ind] ) ;
    index[ind] = -1 ;
    //pthread_mutex_unlock( &indexLock[ind] ) ;

    pthread_mutex_unlock( &indexFinalLock[ind] ) ;

    if( filtering && nbR == 0 )
      {
	if( inSerializer[th] )
	  {
	    pthread_mutex_lock( &serializer );
	    
	    inSerializer[th] = false ;
	    nbThreadPermitted ++ ;
	    pthread_cond_signal( &serialCond );

	    pthread_mutex_unlock( &serializer ) ;
	  }
      }

  }

  
  bool askingIndex(int th, int ind, int nbR)
  {
    //cout << th << " asking " << " " << ind << " " << nbR << endl ;

    nbRessourceAsked = max( nbRessourceAsked, nbR );
    setFiltering() ;
    if( filtering )
      {
	if( ! inSerializer[th] && nbR == 1  ) 
	  {
	    pthread_mutex_lock( &serializer );
	    inSerializer[th] = true ;
	    while( nbThreadPermitted == 0 )
	      {
		//cout << th << " wait " << endl;
	      pthread_cond_wait( &serialCond,&serializer );
	      }
	    //cout << th << " wakes up " << endl ;

	    nbThreadPermitted -- ;
	    pthread_mutex_unlock( &serializer );
	  }
      }

    return askingIndex(th,ind);
  }

  bool askingIndex(int th, int ind)
  {
 
    //cout << th << " asking " << " " << ind << endl ;

    int thStart = th ;
    int indStart = ind ;
    int thEnd ;
    int indEnd ;

    bool result = true ;
    bool correct = false ;
    
    while(true)
      {
	//cout << "b" << endl ;

	th = thStart ; ind = indStart ;
    

	getEnd(th, ind, thStart);
	indEnd = ind ;
	thEnd = th ;


	lock( thStart, thEnd, 0 );
	//pthread_mutex_lock(&serializer);

	th = thStart ;
	ind = indStart ;
	
	getEnd(th, ind, thStart);
	
	if( th == thEnd and ind == indEnd )
	  {
	    
	    if( th != thStart )
	      {
		thread[ thStart ] = indStart ;
		result = true ;
	      }
	    else
	      result = false ;

	    correct = true ;
	  }

	//pthread_mutex_unlock(&serializer) ;
	lock( thStart, thEnd, 1 );
	
	if( correct )
	  break ;
	
      }
    //cout << thStart << " asking " << " " << indStart << " " << result << endl ;
    //if( result == false )
    //  cout << "ARG" << endl ;

    return result ;
  }

 private:
  inline void getEnd(int & th, int & ind, int & thStart)
  {
    while(true)
      {
	if( ind == -1 )
	  break ;
	
	th = index[ind] ;
	
	if( th == -1 or th == thStart )
	  break ;
	
	ind = thread[th] ;	
      }
  }

  inline void lock( int a, int b, int type )
  {
    if( a == b )
      lock( -1, a, type );
    else if( a > b )
      lock( b,a,type );
    else
      {
	if( type == 0 )
	  {
	    if( a >= 0 )
	      pthread_mutex_lock( &threadLock[a] );
	    if( b >= 0 )
	      pthread_mutex_lock( &threadLock[b] );
	  }
	else
	  {
	    if( a >= 0 )
	      pthread_mutex_unlock( &threadLock[a] );
	    if( b >= 0 )
	      pthread_mutex_unlock( &threadLock[b] );
	  }
      }
  }

  
  inline void setFiltering()
  {
    if( nbRessourceAsked > 1 && ! filtering )
      {
	pthread_mutex_lock(&serializer);
	if( ! filtering )
	  {
	    filtering = true ;
	    nbThreadPermitted = max( min(nbIndex / ( nbRessourceAsked + 1 ), 9) , 1 ) ;
	  }
	pthread_mutex_unlock(&serializer);
      }
  }

};


#endif
