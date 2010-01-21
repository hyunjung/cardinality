
#include <stdio.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <fstream>
#include "Tools.h"

using namespace std ;

void removeFile(string f)
{
  if( std::remove( f.c_str() ) != 0 )
    perror( "Error deleting file");
}

double almostNull()
{
  return 0.0001 ;
}

string getStringFromInt( int a )
{
  char buffer[200] ;
  sprintf(buffer,"%d",a);
  return string(buffer) ;
}

string getStringFromValue( Value a )
{
  if( a.type == STRING) 
    return string( a.charVal ); 
  else
    return getStringFromInt( a.intVal ) ;
}

void setValueFromString( const string & str, Value & v )
{
  if( v.type == INT )
  {
    v.intVal = atoi(str.c_str()) ;
  }
  else
    strcpy( v.charVal, str.c_str()) ;
}

FileReader::FileReader(string f) : is(f.c_str()) { }

FileReader::~FileReader(){ is.close(); }

bool FileReader::readable() { return (!is.eof() and is.good()) ; }   

int FileReader::getInt()
{
  int a ;
  is >> a ; 
  return a ;
}

uint64_t FileReader::getLong()
{
  uint64_t a ;
  is >> a ; 
  return a ;
}

string FileReader::getString()
{
  string a ;
  is >> a ; 
  return a ;
}

string FileReader::getLine()
{
  string s ;
  getline( is, s);
  while( ! is.eof() and s.size() == 0 )
    getline( is, s);
  return s ;
}

Value FileReader::getValue(ValueType type)
{
  Value v ;
  v.type = type ;
  if( type == INT )
    v.intVal = getInt() ;
  else
    strcpy(v.charVal, getString().c_str() ) ;

  return v ;
}

string FileReader::getStripLine()
{
  string s = getLine() ;
  if( s[s.size()-1] == '\n')
    return s.substr(0,s.size()-1) ;
  else 
    return s ;
}

string FileReader::getFullFile()
{
  int length = 1000000;
  char buffer[1000000];

  is.read(buffer,length);
  string s(buffer);

  return s ;
}

void splitMultiple( const string & in, const vector<string> & separators, vector<string> & out )
{
  int startp = 0 ;

  while(true)
  {
    string::size_type p = string::npos ;
    int chosenS = 0 ;
    for( int s = 0 ; s < separators.size() ; s ++ )
    {
      string::size_type possP = in.find(separators[s],startp) ;
      if( possP != string::npos and ( p == string::npos or possP < p ) )
      {
        p = possP ;
        chosenS = s ;
      }
    }

    if( p == string::npos )
    {
      out.push_back( in.substr(startp, in.size()-startp ) );
      return ;
    }
    else
    {
      out.push_back( in.substr(startp,p - startp) );
      startp = p + separators[chosenS].size() ;
    }
  }
}

void split( const string & in, const string & separator, std::vector<string> & out )
{
  int startp = 0 ;

  while(true)
    {
    int p = in.find(separator,startp) ;
    if( p == string::npos )
      {
        out.push_back( in.substr(startp, in.size()-startp ) );
        return ;
      }
    else
      {
        out.push_back( in.substr(startp,p - startp) );
        startp = p + separator.size() ;
      }
    }
}


void splitTerms( const string & inReal, std::vector<string> & out )
{
  string in = inReal + " " ;

  bool inString = false ;
  bool inQuote = false ;
  int startString = -1 ;
  int nbAntiSlash = 0 ;

  for( int i = 0 ; i < in.size() ; i ++ )
  {
    if( (! inString) and in[i] != ' ')
    {
      startString = i ;
      inString = true ;
      inQuote = (in[i] == '\'') ;
    }
    else if( inString and (! inQuote) and in[i] == ' ' )
    {
      inString = false ;
      //cout << in.substr( startString, i-startString ) << endl ;
      out.push_back(in.substr( startString, i-startString ) );
    }
    else if( inString and inQuote and in[i] == '\'' )
    {
      if( nbAntiSlash % 2 == 0 )
      {
        out.push_back(in.substr( startString, i-startString+1) ) ; 
        //cout << in.substr( startString, i-startString+1 ) << endl ;
        inString = false ;
        inQuote = false ;
      }
    }

    if( in[i] == '\\' )
      nbAntiSlash += 1 ;
    else
      nbAntiSlash = 0 ;
  }

}

string trim( const string & in )
{
  string out = in ;
  while( out.size() > 0 )
    {
    char c = out[0] ;
    if( c == ' ' or c == '\n' or c =='\r' or c=='\t' )
      out = out.substr(1, out.size()-1 ) ;
    else
      break ;
    }
  while( out.size() > 0 )
    {
    char c = out[out.size()-1] ;
    if( c == ' ' or c == '\n' or c =='\r' or c=='\t' )
      out = out.substr(0, out.size()-1 ) ;
    else
      break ;
    }

  return out ;
}

string replace(string in, string search, string replaceby)
{
  vector<string> temp ;
  split(in,search,temp);
  string out = temp[0] ;
  for( int c = 1 ; c < temp.size() ; c ++ )
    out += replaceby + temp[c] ; 
  return out ;
}

void splitTrim( const string & in, const string & separator, vector<string> & out )
{
  vector<string> outPre ;
  split( in, separator, outPre ) ;
  for( int op = 0 ; op < outPre.size() ; op ++ )
  {
    string term = trim( outPre[op] );
    if( term.size() > 0 )
      out.push_back(term) ;
  }

}

int hashValue(Value & v)
{
  if( v.type == INT )
    return v.intVal ;
  else
  {
    int hash,i ;
    int len = strlen( v.charVal );
    for (hash=0, i=0; i<len; ++i)
    {
      hash += v.charVal[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);

    if( hash < 0 )
      return -hash ;
    else
      return hash ;
  }  

}


