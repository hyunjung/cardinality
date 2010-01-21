
import sys

if len( sys.argv ) < 3:
    print "Usage : python AddTestFile.py NameClass FirstTestName"
    exit()


TestFile = """
#include <cppunit/extensions/HelperMacros.h>

using namespace std ;

class TestNAME : public CppUnit::TestFixture  {

  CPPUNIT_TEST_SUITE( TestNAME );
  CPPUNIT_TEST( testFIRSTTEST );
  CPPUNIT_TEST_SUITE_END();

public:
  void testFIRSTTEST()
  {

  }


};

CPPUNIT_TEST_SUITE_REGISTRATION( TestNAME );
"""

TestFile = TestFile.replace("NAME",sys.argv[1])
TestFile = TestFile.replace("FIRSTTEST",sys.argv[2])

open("Test"+sys.argv[1]+".cpp","w").write(TestFile)
