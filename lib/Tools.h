
#ifndef __AUTOANSWER__TOOLS__
#define __AUTOANSWER__TOOLS__

#include <stdio.h>
#include <string>
#include <iostream>
#include <fstream>
#include "../include/client.h"
#include <vector>

/**
 * The following are currently used in the sample clients and the benchmark. 
 * You do not have to use these methods.
 **/

using namespace std ;

/**
 * Remove a file, used for unit-testing purpose
 */
void removeFile(std::string f);

/**
 * Classic split function of a string
 */
void split( const std::string & in, const std::string & separator, std::vector<std::string> & out );

/**
 * Classic replace function for a string
 */
string replace(string in, string search, string replaceby);

/**
 * Split function with multiple separators
 */
void splitMultiple( const std::string & in, const vector<string> & separator, std::vector<std::string> & out );

/**
 * Split function with a single separator but trim the resulting part
 */
void splitTrim( const std::string & in, const string & separator, std::vector<std::string> & out );

/**
 * Split function for the query parsing
 */
void splitTerms( const std::string & in, std::vector<std::string> & out );

/**
 * Trim a string ( remove white spaces at the beginning and the end ).
 */
string trim( const string & in );

/**
 * Methods to handle the Value type
 */
string getStringFromInt( int a );
string getStringFromValue( Value a );
void setValueFromString( const string & str, Value & v );

/**
 * Return a double very near 0, for equal double purpose
 */
double almostNull();

/**
 * Return a deterministic hash from a value
 */
int hashValue(Value & v);

/**
 * Class used for reading from files
 * Not super stable, may be improved
 */
class FileReader
{
 private:
  std::ifstream is ;

 public:
  FileReader(std::string fileName); 
  ~FileReader();

  /**
   * Return true if there is more to read on the file
   */
  bool readable();
  
  /**
   * Return data from the file
   */
  int getInt();
  uint64_t getLong();
  string getString() ;
  Value getValue(ValueType type);  
 
  /**
   * Return a full line with the \n at the end
   * If the line read is only \n, will read an additionnal line 
   */
  std::string getLine();
  
  /**
   * Return a full line but without the \n at the end
   * If the line read is only \n, will read an additionnal line 
   */
  std::string getStripLine();
  
  /**
   * Return the full file
   */
  std::string getFullFile();
};

#endif
