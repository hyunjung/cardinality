
#include <cppunit/extensions/HelperMacros.h>
#include "../bench/clientHelper.h"

using namespace std ;

class TestBench : public CppUnit::TestFixture  {

  CPPUNIT_TEST_SUITE( TestBench );
  CPPUNIT_TEST( testBasicCalls );
  CPPUNIT_TEST_SUITE_END();

public:
  void testBasicCalls()
  {
    string testDir = "dataUnitTest/TestBench_BasicCalls" ;
  
    vector<Node > nodes ;
    int nbSeconds ;
    Data data = createDataFromDirectory(testDir,nodes, nbSeconds) ;

    CPPUNIT_ASSERT_EQUAL(2,(int) nodes.size()) ;
    CPPUNIT_ASSERT_EQUAL(2,data.nbTables) ;
    CPPUNIT_ASSERT_EQUAL(0,strcmp(data.tables[0].tableName,"primes")) ;
    CPPUNIT_ASSERT_EQUAL(2,data.tables[1].nbPartitions) ;

    deleteData(data) ;

    vector<Query> queries;
    vector<Query> preset;
    uint64_t rows, hash;
    createQueryFromDirectory(testDir, preset,queries,rows,hash);

    CPPUNIT_ASSERT_EQUAL(2, queries[0].nbOutputFields);
    CPPUNIT_ASSERT_EQUAL(1, queries[0].nbJoins);
    CPPUNIT_ASSERT_EQUAL(2, (int) preset.size());
    CPPUNIT_ASSERT_EQUAL(6, (int) queries[0].restrictionGreaterThanValues[0].intVal);
    CPPUNIT_ASSERT_EQUAL(0, strcmp(queries[0].restrictionGreaterThanValues[1].charVal, "veryfunny" ));
    CPPUNIT_ASSERT_EQUAL(0, strcmp(queries[1].restrictionGreaterThanValues[1].charVal, "clems n'est pas la" ));

    deleteQuery(queries[0]) ;
    deleteQuery(queries[1]) ;
  }


};

CPPUNIT_TEST_SUITE_REGISTRATION( TestBench );
