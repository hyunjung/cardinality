
#include <cppunit/extensions/HelperMacros.h>
#include "../lib/Tools.h"
#include "../lib/index/include/server.h"
#include <cstring>

using namespace std ;

class TestTools : public CppUnit::TestFixture  {

  CPPUNIT_TEST_SUITE( TestTools );
  CPPUNIT_TEST( testHash );
  CPPUNIT_TEST( testIndex );
  CPPUNIT_TEST_SUITE_END();

public:
  void testHash()
  {
    Value v ;
    v.type = INT ;
    v.intVal = 42 ;
    CPPUNIT_ASSERT_EQUAL( 42, hashValue( v ) ) ;    
    v.intVal = 0 ;
    CPPUNIT_ASSERT_EQUAL( 0, hashValue( v ) ) ;    

    v.type = STRING ;
    strcpy( v.charVal, "clement" ) ;
    CPPUNIT_ASSERT_EQUAL( 979945711, hashValue( v ) ) ;    
  }

  void testIndex()
  {
    Index *idx;
    create(STRING, "idx1");
    openIndex("idx1", &idx);
    for (int i = 65; i < 65 + 26; i++) {       
      Value v;       
      memset(v.charVal, 'A', 5);
      v.charVal[5] = i; 
      v.charVal[6] = 0x0; 
      insertRecord(idx, NULL, (Key*) &v, i);
    }      
    for (int i = 65; i < 65 + 26; i++) { 
      Value v; 
      memset(v.charVal, 'A', 15); 
      v.charVal[15] = i; 
      v.charVal[16] = 0x0; 
      insertRecord(idx, NULL, (Key*) &v, i); 
    } 
  
    Value v; 
    strcpy(v.charVal, "091");
    insertRecord(idx, NULL, (Key*) &v, 12); 
    strcpy(v.charVal, "0835");
    insertRecord(idx, NULL, (Key*) &v, 12); 

    TxnState *txn; 
    Record record; 
    beginTransaction(&txn); 
    cout << " -- Index -- " << endl; 
    char prec[200] ;
    bool first = true ;
    while(getNext(idx, txn, &record) != DB_END) {   
      if ( ! first )
        CPPUNIT_ASSERT( strcmp( record.val.charVal , prec ) > 0 ) ;
      first = false ;
      strcpy( prec, record.val.charVal );
      cout << record.val.charVal << " : " << record.address << endl;
    } 



  }

};

CPPUNIT_TEST_SUITE_REGISTRATION( TestTools );
