#include "clientIndex.h" 
#include "../lib/Tools.h" 
#include <sstream> 

using namespace std ;

void createIndex( char * indexName, char * fileName, int iIndex, ValueType indexType )
{
  ErrCode ret;
  ret = create( indexType, indexName ) ;
  assert( SUCCESS == ret );
  Index * index ;
  ret = openIndex( indexName, & index ) ;
  assert( SUCCESS == ret );

  FileReader fr(fileName) ;

  uint64_t iLine = 0 ; 
  while( true )
  {
    string line = fr.getStripLine() ;
    if( line.size() == 0 )
      break ;
    else
    {
      vector<string> parts ;
      split(line,"|",parts);
      assert( parts.size() > iIndex );

      Value v ;
      v.type = indexType ;
      setValueFromString( parts[iIndex] , v ); 

      #if DEBUG >= 3
      cout << v.intVal << " " << iLine << endl ;
      #endif

      insertRecord( index, NULL, (Key*) &v, iLine );
    }

    iLine += line.size() +1 ; 
  } 
}
